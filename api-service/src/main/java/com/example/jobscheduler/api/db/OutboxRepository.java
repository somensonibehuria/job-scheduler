package com.example.jobscheduler.api.db;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;

  public OutboxRepository(JdbcTemplate jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
  }

  public void enqueue(String topic, String key, Object payload) {
    JsonNode json = mapper.valueToTree(payload);
    jdbc.update(
        "INSERT INTO outbox (topic, message_key, payload) VALUES (?, ?, ?::jsonb)",
        topic, key, json.toString()
    );
  }

  public List<OutboxRow> claimBatch(String lockOwner, int limit) {
    return jdbc.query(
        """
        WITH cte AS (
          SELECT id
          FROM outbox
          WHERE published_at IS NULL
            AND (locked_at IS NULL OR locked_at < (now() - interval '30 seconds'))
          ORDER BY created_at
          FOR UPDATE SKIP LOCKED
          LIMIT ?
        )
        UPDATE outbox o
        SET locked_at = now(), lock_owner = ?
        FROM cte
        WHERE o.id = cte.id
        RETURNING o.id, o.topic, o.message_key, o.payload
        """,
        (rs, rowNum) -> new OutboxRow(
            rs.getLong("id"),
            rs.getString("topic"),
            rs.getString("message_key"),
            rs.getString("payload")
        ),
        limit, lockOwner
    );
  }

  public void markPublished(long id) {
    jdbc.update("UPDATE outbox SET published_at = ?, locked_at = NULL, lock_owner = NULL WHERE id = ?",
        Instant.now(), id);
  }

  public record OutboxRow(long id, String topic, String key, String payloadJson) {}
}

