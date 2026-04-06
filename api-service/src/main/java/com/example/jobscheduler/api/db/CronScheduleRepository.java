package com.example.jobscheduler.api.db;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CronScheduleRepository {
  private final JdbcTemplate jdbc;

  public CronScheduleRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public UUID upsert(UUID scheduleId, UUID jobDefinitionId, String cronExpr, String timezone, Instant nextFireAt) {
    jdbc.update(
        """
        INSERT INTO cron_schedule (id, job_definition_id, cron_expr, timezone, next_fire_at, enabled)
        VALUES (?, ?, ?, ?, ?, true)
        ON CONFLICT (id) DO UPDATE SET
          job_definition_id = EXCLUDED.job_definition_id,
          cron_expr = EXCLUDED.cron_expr,
          timezone = EXCLUDED.timezone,
          next_fire_at = EXCLUDED.next_fire_at,
          enabled = true,
          version = cron_schedule.version + 1
        """,
        scheduleId, jobDefinitionId, cronExpr, timezone, nextFireAt
    );
    return scheduleId;
  }

  public Optional<CronRow> get(UUID id) {
    return jdbc.query(
        "SELECT id, job_definition_id, cron_expr, timezone, next_fire_at, enabled FROM cron_schedule WHERE id = ?",
        (rs, rowNum) -> new CronRow(
            UUID.fromString(rs.getString("id")),
            UUID.fromString(rs.getString("job_definition_id")),
            rs.getString("cron_expr"),
            rs.getString("timezone"),
            rs.getTimestamp("next_fire_at").toInstant(),
            rs.getBoolean("enabled")
        ),
        id
    ).stream().findFirst();
  }

  public record CronRow(UUID id, UUID jobDefinitionId, String cronExpr, String timezone, Instant nextFireAt, boolean enabled) {}
}

