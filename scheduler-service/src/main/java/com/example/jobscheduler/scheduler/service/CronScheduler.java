package com.example.jobscheduler.scheduler.service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import com.example.jobscheduler.common.TopicNames;
import com.example.jobscheduler.common.messages.ExecutionReadyMessage;
import com.example.jobscheduler.scheduler.db.OutboxRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class CronScheduler {
  private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);

  private final JdbcTemplate jdbc;
  private final OutboxRepository outbox;

  public CronScheduler(JdbcTemplate jdbc, OutboxRepository outbox) {
    this.jdbc = jdbc;
    this.outbox = outbox;
  }

  @Scheduled(fixedDelayString = "${cron.scan.delay-ms:250}")
  public void tick() {
    try {
      processDue(250);
    } catch (Exception e) {
      log.warn("cron scan failed", e);
    }
  }

  @Transactional
  public void processDue(int batchSize) {
    List<CronRow> due = jdbc.query(
        """
        SELECT cs.id, cs.job_definition_id, cs.cron_expr, cs.timezone, cs.next_fire_at, jd.default_max_attempts, jd.default_timeout_ms
        FROM cron_schedule cs
        JOIN job_definition jd ON jd.id = cs.job_definition_id
        WHERE cs.enabled = true AND cs.next_fire_at <= now()
        ORDER BY cs.next_fire_at
        FOR UPDATE SKIP LOCKED
        LIMIT ?
        """,
        (rs, rowNum) -> new CronRow(
            UUID.fromString(rs.getString("id")),
            UUID.fromString(rs.getString("job_definition_id")),
            rs.getString("cron_expr"),
            rs.getString("timezone"),
            rs.getTimestamp("next_fire_at").toInstant(),
            rs.getInt("default_max_attempts"),
            rs.getInt("default_timeout_ms")
        ),
        batchSize
    );

    for (CronRow row : due) {
      Instant fireAt = row.nextFireAt();
      Instant nextFire = computeNext(row.cronExpr(), row.timezone(), fireAt);

      UUID executionId = UUID.randomUUID();
      jdbc.update(
          """
          INSERT INTO job_execution (id, job_definition_id, execution_type, status, scheduled_for, max_attempts, timeout_ms)
          VALUES (?, ?, 'CRON', 'QUEUED', ?, ?, ?)
          """,
          executionId, row.jobDefinitionId(), fireAt, row.defaultMaxAttempts(), row.defaultTimeoutMs()
      );
      outbox.enqueue(TopicNames.EXECUTIONS_READY, executionId.toString(), new ExecutionReadyMessage(executionId));

      jdbc.update("UPDATE cron_schedule SET next_fire_at = ? WHERE id = ?", nextFire, row.id());
    }
  }

  private Instant computeNext(String cronExpr, String timezone, Instant from) {
    CronExpression expr = CronExpression.parse(cronExpr);
    ZoneId zone = ZoneId.of(timezone);
    ZonedDateTime base = ZonedDateTime.ofInstant(from, zone).plusSeconds(1);
    ZonedDateTime next = expr.next(base);
    if (next == null) {
      // Should not happen for valid cron; fallback to +60s.
      return base.plusSeconds(60).toInstant();
    }
    return next.toInstant();
  }

  private record CronRow(
      UUID id,
      UUID jobDefinitionId,
      String cronExpr,
      String timezone,
      Instant nextFireAt,
      int defaultMaxAttempts,
      int defaultTimeoutMs
  ) {}
}
