package com.example.jobscheduler.api.service;

import com.example.jobscheduler.api.db.JobDefinitionRepository;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class DefaultJobBootstrap {
  public static final String DEFAULT_CRON_JOB_NAME = "default-cron";
  public static final String DEFAULT_ADHOC_JOB_NAME = "default-adhoc";

  private final JobDefinitionRepository jobDefinitions;

  public DefaultJobBootstrap(JobDefinitionRepository jobDefinitions) {
    this.jobDefinitions = jobDefinitions;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void ensureDefaultsExist() {
    jobDefinitions.upsertDefault(DEFAULT_CRON_JOB_NAME, "sampleCron", 5, 30_000);
    jobDefinitions.upsertDefault(DEFAULT_ADHOC_JOB_NAME, "sampleAdhoc", 3, 30_000);
  }
}

