package com.example.jobscheduler.scheduler.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.example.jobscheduler.common.TopicNames;
import com.example.jobscheduler.common.messages.ExecutionReadyMessage;
import com.example.jobscheduler.scheduler.db.OutboxRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DelayDispatcher {
  private static final Logger log = LoggerFactory.getLogger(DelayDispatcher.class);
  private static final String ZSET_KEY = "delayed:executions";

  private final StringRedisTemplate redis;
  private final OutboxRepository outbox;

  public DelayDispatcher(StringRedisTemplate redis, OutboxRepository outbox) {
    this.redis = redis;
    this.outbox = outbox;
  }

  @Scheduled(fixedDelayString = "${delay.dispatch.delay-ms:100}")
  public void tick() {
    try {
      dispatchDue(500);
    } catch (Exception e) {
      log.warn("delay dispatch failed", e);
    }
  }

  @Transactional
  public void dispatchDue(int limit) {
    long now = Instant.now().toEpochMilli();
    var ops = redis.opsForZSet();
    var due = ops.rangeByScore(ZSET_KEY, 0, now, 0, limit);
    if (due == null || due.isEmpty()) return;

    List<String> toRemove = new ArrayList<>(due);
    Long removed = ops.remove(ZSET_KEY, toRemove.toArray());
    if (removed == null || removed == 0) return;

    for (String executionId : due) {
      try {
        UUID id = UUID.fromString(executionId);
        outbox.enqueue(TopicNames.EXECUTIONS_READY, id.toString(), new ExecutionReadyMessage(id));
      } catch (Exception e) {
        log.warn("bad delayed execution id={}", executionId, e);
      }
    }
  }
}

