package com.example.jobscheduler.api.db;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobExecutionRepository {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;

  public JobExecutionRepository(JdbcTemplate jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
  }

  public ExecutionRow insert(UUID executionId, UUID jobDefinitionId, String executionType, String status,
      Instant scheduledFor, int maxAttempts, int timeoutMs, Object payload) {
    String payloadJson = payload == null ? null : mapper.valueToTree(payload).toString();
    jdbc.update(
        """
        INSERT INTO job_execution (id, job_definition_id, execution_type, status, scheduled_for, max_attempts, timeout_ms, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)
        """,
        executionId, jobDefinitionId, executionType, status, scheduledFor, maxAttempts, timeoutMs, payloadJson
    );
    return get(executionId).orElseThrow();
  }

  public Optional<ExecutionRow> get(UUID id) {
    return jdbc.query(
        """
        SELECT id, job_definition_id, execution_type, status, scheduled_for, attempt, max_attempts, timeout_ms,
               payload, lease_owner, lease_until, started_at, finished_at, last_error, created_at
        FROM job_execution
        WHERE id = ?
        """,
        (rs, rowNum) -> new ExecutionRow(
            UUID.fromString(rs.getString("id")),
            UUID.fromString(rs.getString("job_definition_id")),
            rs.getString("execution_type"),
            rs.getString("status"),
            rs.getTimestamp("scheduled_for").toInstant(),
            rs.getInt("attempt"),
            rs.getInt("max_attempts"),
            rs.getInt("timeout_ms"),
            toJsonNode(rs.getString("payload")),
            rs.getString("lease_owner"),
            rs.getTimestamp("lease_until") == null ? null : rs.getTimestamp("lease_until").toInstant(),
            rs.getTimestamp("started_at") == null ? null : rs.getTimestamp("started_at").toInstant(),
            rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
            rs.getString("last_error"),
            rs.getTimestamp("created_at").toInstant()
        ),
        id
    ).stream().findFirst();
  }

  public List<ExecutionRow> listRecent(int limit) {
    return jdbc.query(
        """
        SELECT id, job_definition_id, execution_type, status, scheduled_for, attempt, max_attempts, timeout_ms,
               payload, lease_owner, lease_until, started_at, finished_at, last_error, created_at
        FROM job_execution
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (rs, rowNum) -> new ExecutionRow(
            UUID.fromString(rs.getString("id")),
            UUID.fromString(rs.getString("job_definition_id")),
            rs.getString("execution_type"),
            rs.getString("status"),
            rs.getTimestamp("scheduled_for").toInstant(),
            rs.getInt("attempt"),
            rs.getInt("max_attempts"),
            rs.getInt("timeout_ms"),
            toJsonNode(rs.getString("payload")),
            rs.getString("lease_owner"),
            rs.getTimestamp("lease_until") == null ? null : rs.getTimestamp("lease_until").toInstant(),
            rs.getTimestamp("started_at") == null ? null : rs.getTimestamp("started_at").toInstant(),
            rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
            rs.getString("last_error"),
            rs.getTimestamp("created_at").toInstant()
        ),
        limit
    );
  }

  private JsonNode toJsonNode(String payloadJson) {
    if (payloadJson == null) return null;
    try {
      return mapper.readTree(payloadJson);
    } catch (Exception e) {
      return null;
    }
  }

  public record ExecutionRow(
      UUID id,
      UUID jobDefinitionId,
      String executionType,
      String status,
      Instant scheduledFor,
      int attempt,
      int maxAttempts,
      int timeoutMs,
      JsonNode payload,
      String leaseOwner,
      Instant leaseUntil,
      Instant startedAt,
      Instant finishedAt,
      String lastError,
      Instant createdAt
  ) {}
}

