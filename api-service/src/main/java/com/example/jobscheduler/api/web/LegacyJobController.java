package com.example.jobscheduler.api.web;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.example.jobscheduler.api.db.JobExecutionRepository;
import com.example.jobscheduler.api.service.JobSubmissionService;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
public class LegacyJobController {
  private final JobSubmissionService submitter;
  private final JobExecutionRepository executions;

  public LegacyJobController(JobSubmissionService submitter, JobExecutionRepository executions) {
    this.submitter = submitter;
    this.executions = executions;
  }

  @PostMapping("/cron/run")
  public ResponseEntity<Map<String, Object>> runCronNow() {
    UUID id = submitter.submitDefaultCronNow();
    return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("executionId", id));
  }

  public record AdhocRequest(@Size(max = 2000) String payload) {}

  @PostMapping("/adhoc/run")
  public ResponseEntity<Map<String, Object>> runAdhocNow(@Valid @RequestBody(required = false) AdhocRequest body) {
    Object payload = body == null ? null : Map.of("payload", body.payload());
    UUID id = submitter.submitDefaultAdhocNow(payload);
    return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("executionId", id));
  }

  @GetMapping("/{id}")
  public ResponseEntity<JobExecutionRepository.ExecutionRow> get(@PathVariable UUID id) {
    return executions.get(id).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
  }

  @GetMapping
  public List<JobExecutionRepository.ExecutionRow> list() {
    return executions.listRecent(200);
  }
}

