package com.example.jobscheduler.api.db;

import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobDefinitionRepository {
  private final JdbcTemplate jdbc;

  public JobDefinitionRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public UUID upsertDefault(String name, String handler, int maxAttempts, int timeoutMs) {
    UUID id = deterministicIdForName(name);
    jdbc.update(
        """
        INSERT INTO job_definition (id, name, handler, default_max_attempts, default_timeout_ms)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          handler = EXCLUDED.handler,
          default_max_attempts = EXCLUDED.default_max_attempts,
          default_timeout_ms = EXCLUDED.default_timeout_ms
        """,
        id, name, handler, maxAttempts, timeoutMs
    );
    return id;
  }

  public Optional<JobDefinitionRow> get(UUID id) {
    return jdbc.query(
        "SELECT id, name, handler, default_max_attempts, default_timeout_ms FROM job_definition WHERE id = ?",
        (rs, rowNum) -> new JobDefinitionRow(
            UUID.fromString(rs.getString("id")),
            rs.getString("name"),
            rs.getString("handler"),
            rs.getInt("default_max_attempts"),
            rs.getInt("default_timeout_ms")
        ),
        id
    ).stream().findFirst();
  }

  private static UUID deterministicIdForName(String name) {
    return UUID.nameUUIDFromBytes(("job-def:" + name).getBytes());
  }

  public record JobDefinitionRow(UUID id, String name, String handler, int defaultMaxAttempts, int defaultTimeoutMs) {}
}

