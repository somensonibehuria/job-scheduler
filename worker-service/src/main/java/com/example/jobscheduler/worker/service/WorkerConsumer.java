package com.example.jobscheduler.worker.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.example.jobscheduler.common.TopicNames;
import com.example.jobscheduler.common.messages.ExecutionReadyMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class WorkerConsumer {
  private static final Logger log = LoggerFactory.getLogger(WorkerConsumer.class);

  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final JobHandlers handlers;
  private final DelayQueueClient delayQueue;
  private final String workerId = "worker-" + UUID.randomUUID();

  public WorkerConsumer(JdbcTemplate jdbc, ObjectMapper mapper, JobHandlers handlers, DelayQueueClient delayQueue) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.handlers = handlers;
    this.delayQueue = delayQueue;
  }

  @KafkaListener(topics = TopicNames.EXECUTIONS_READY, groupId = "${worker.group-id:job-scheduler-workers}")
  public void onMessage(String payloadJson) throws Exception {
    ExecutionReadyMessage msg = mapper.readValue(payloadJson, ExecutionReadyMessage.class);
    process(msg.executionId());
  }

  @Transactional
  public void process(UUID executionId) {
    Optional<ExecutionRow> claimed = claim(executionId);
    if (claimed.isEmpty()) return;

    ExecutionRow row = claimed.get();
    try {
      handlers.run(row.handler(), row.payload());
      markSuccess(row.executionId());
    } catch (Exception e) {
      handleFailure(row, e);
    }
  }

  private Optional<ExecutionRow> claim(UUID executionId) {
    return jdbc.query(
        """
        UPDATE job_execution je
        SET status = 'RUNNING',
            lease_owner = ?,
            lease_until = now() + (? || ' milliseconds')::interval,
            started_at = COALESCE(started_at, now()),
            last_error = NULL
        FROM job_definition jd
        WHERE je.id = ?
          AND jd.id = je.job_definition_id
          AND je.status IN ('QUEUED','RETRY')
          AND (je.lease_until IS NULL OR je.lease_until < now())
        RETURNING je.id AS execution_id, jd.handler, je.payload, je.attempt, je.max_attempts
        """,
        (rs, rowNum) -> new ExecutionRow(
            UUID.fromString(rs.getString("execution_id")),
            rs.getString("handler"),
            toJsonNode(rs.getString("payload")),
            rs.getInt("attempt"),
            rs.getInt("max_attempts")
        ),
        workerId, 30_000, executionId
    ).stream().findFirst();
  }

  private void markSuccess(UUID executionId) {
    jdbc.update(
        """
        UPDATE job_execution
        SET status='SUCCESS', finished_at=now(), lease_owner=NULL, lease_until=NULL
        WHERE id=?
        """,
        executionId
    );
    jdbc.update("INSERT INTO job_event (execution_id, event_type, details) VALUES (?, 'SUCCESS', '{}'::jsonb)", executionId);
  }

  private void handleFailure(ExecutionRow row, Exception e) {
    int nextAttempt = row.attempt() + 1;
    boolean willRetry = nextAttempt < row.maxAttempts();
    String error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();

    jdbc.update(
        """
        UPDATE job_execution
        SET status = ?,
            attempt = ?,
            finished_at = now(),
            last_error = ?,
            lease_owner = NULL,
            lease_until = NULL
        WHERE id = ?
        """,
        willRetry ? "RETRY" : "FAILED",
        nextAttempt,
        error,
        row.executionId()
    );
    jdbc.update(
        "INSERT INTO job_event (execution_id, event_type, details) VALUES (?, ?, jsonb_build_object('error', ?))",
        row.executionId(),
        willRetry ? "RETRY" : "FAILED",
        error
    );

    if (willRetry) {
      Instant runAt = Instant.now().plus(computeBackoff(nextAttempt));
      delayQueue.schedule(row.executionId().toString(), runAt);
    } else {
      log.warn("execution failed permanently id={} error={}", row.executionId(), error);
    }
  }

  private Duration computeBackoff(int attempt) {
    long baseMs = 100;
    long maxMs = 300_000;
    long exp = Math.min(maxMs, baseMs * (1L << Math.min(20, attempt)));
    long jitter = (long) (Math.random() * 250);
    return Duration.ofMillis(Math.min(maxMs, exp + jitter));
  }

  private JsonNode toJsonNode(String payloadJson) {
    if (payloadJson == null) return null;
    try {
      return mapper.readTree(payloadJson);
    } catch (Exception e) {
      return null;
    }
  }

  private record ExecutionRow(UUID executionId, String handler, JsonNode payload, int attempt, int maxAttempts) {}
}

