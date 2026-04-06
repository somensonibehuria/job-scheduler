package com.example.jobscheduler.scheduler.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HistoryRetentionJob {
  private static final Logger log = LoggerFactory.getLogger(HistoryRetentionJob.class);

  private final JdbcTemplate jdbc;

  public HistoryRetentionJob(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Scheduled(cron = "${history.retention.cron:0 0 3 * * *}")
  public void prune() {
    // Skeleton retention: delete rows older than 365 days.
    // For production scale, prefer time-partitioned tables and drop old partitions.
    int events = jdbc.update("DELETE FROM job_event WHERE ts < now() - interval '365 days'");
    int execs = jdbc.update("DELETE FROM job_execution WHERE created_at < now() - interval '365 days' AND status IN ('SUCCESS','FAILED')");
    if (events > 0 || execs > 0) {
      log.info("retention pruned events={} executions={}", events, execs);
    }
  }
}

