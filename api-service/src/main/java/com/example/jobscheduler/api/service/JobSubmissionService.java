package com.example.jobscheduler.api.service;

import java.time.Instant;
import java.util.UUID;

import com.example.jobscheduler.api.db.JobDefinitionRepository;
import com.example.jobscheduler.api.db.JobExecutionRepository;
import com.example.jobscheduler.api.db.OutboxRepository;
import com.example.jobscheduler.common.TopicNames;
import com.example.jobscheduler.common.messages.ExecutionReadyMessage;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobSubmissionService {
  private final JobDefinitionRepository jobDefinitions;
  private final JobExecutionRepository executions;
  private final OutboxRepository outbox;

  public JobSubmissionService(JobDefinitionRepository jobDefinitions, JobExecutionRepository executions, OutboxRepository outbox) {
    this.jobDefinitions = jobDefinitions;
    this.executions = executions;
    this.outbox = outbox;
  }

  @Transactional
  public UUID submitDefaultCronNow() {
    UUID jobDefinitionId = jobDefinitions
        .upsertDefault(DefaultJobBootstrap.DEFAULT_CRON_JOB_NAME, "sampleCron", 5, 30_000);
    UUID executionId = UUID.randomUUID();
    int maxAttempts = jobDefinitions.get(jobDefinitionId).orElseThrow().defaultMaxAttempts();
    int timeoutMs = jobDefinitions.get(jobDefinitionId).orElseThrow().defaultTimeoutMs();

    executions.insert(executionId, jobDefinitionId, "CRON", "QUEUED", Instant.now(), maxAttempts, timeoutMs, null);
    outbox.enqueue(TopicNames.EXECUTIONS_READY, executionId.toString(), new ExecutionReadyMessage(executionId));
    return executionId;
  }

  @Transactional
  public UUID submitDefaultAdhocNow(Object payload) {
    UUID jobDefinitionId = jobDefinitions
        .upsertDefault(DefaultJobBootstrap.DEFAULT_ADHOC_JOB_NAME, "sampleAdhoc", 3, 30_000);
    UUID executionId = UUID.randomUUID();
    int maxAttempts = jobDefinitions.get(jobDefinitionId).orElseThrow().defaultMaxAttempts();
    int timeoutMs = jobDefinitions.get(jobDefinitionId).orElseThrow().defaultTimeoutMs();

    executions.insert(executionId, jobDefinitionId, "ADHOC", "QUEUED", Instant.now(), maxAttempts, timeoutMs, payload);
    outbox.enqueue(TopicNames.EXECUTIONS_READY, executionId.toString(), new ExecutionReadyMessage(executionId));
    return executionId;
  }
}

