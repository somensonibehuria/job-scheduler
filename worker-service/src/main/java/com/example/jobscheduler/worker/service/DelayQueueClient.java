package com.example.jobscheduler.worker.service;

import java.time.Instant;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class DelayQueueClient {
  private static final String ZSET_KEY = "delayed:executions";

  private final StringRedisTemplate redis;

  public DelayQueueClient(StringRedisTemplate redis) {
    this.redis = redis;
  }

  public void schedule(String executionId, Instant runAt) {
    redis.opsForZSet().add(ZSET_KEY, executionId, runAt.toEpochMilli());
  }
}

