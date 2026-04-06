package com.example.jobscheduler.api.web;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import com.example.jobscheduler.api.db.CronScheduleRepository;
import com.example.jobscheduler.api.db.JobDefinitionRepository;
import com.example.jobscheduler.api.service.DefaultJobBootstrap;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/schedules")
public class CronScheduleController {
  private final JobDefinitionRepository jobDefinitions;
  private final CronScheduleRepository schedules;

  public CronScheduleController(JobDefinitionRepository jobDefinitions, CronScheduleRepository schedules) {
    this.jobDefinitions = jobDefinitions;
    this.schedules = schedules;
  }

  public record UpsertCronRequest(@NotBlank String cronExpr, @NotBlank String timezone) {}

  @PostMapping("/cron/default")
  public ResponseEntity<Map<String, Object>> upsertDefaultCron(@Valid @RequestBody UpsertCronRequest req) {
    UUID jobDefinitionId = jobDefinitions.upsertDefault(DefaultJobBootstrap.DEFAULT_CRON_JOB_NAME, "sampleCron", 5, 30_000);
    UUID scheduleId = UUID.nameUUIDFromBytes(("cron:" + DefaultJobBootstrap.DEFAULT_CRON_JOB_NAME).getBytes());
    Instant nextFireAt = computeNext(req.cronExpr(), req.timezone(), Instant.now());
    schedules.upsert(scheduleId, jobDefinitionId, req.cronExpr(), req.timezone(), nextFireAt);
    return ResponseEntity.ok(Map.of("scheduleId", scheduleId, "nextFireAt", nextFireAt));
  }

  @GetMapping("/cron/default")
  public ResponseEntity<?> getDefaultCron() {
    UUID scheduleId = UUID.nameUUIDFromBytes(("cron:" + DefaultJobBootstrap.DEFAULT_CRON_JOB_NAME).getBytes());
    return schedules.get(scheduleId).<ResponseEntity<?>>map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.ok(Map.of("enabled", false)));
  }

  private Instant computeNext(String cronExpr, String timezone, Instant from) {
    CronExpression expr = CronExpression.parse(cronExpr);
    ZoneId zone = ZoneId.of(timezone);
    ZonedDateTime base = ZonedDateTime.ofInstant(from, zone).plusSeconds(1);
    ZonedDateTime next = expr.next(base);
    if (next == null) {
      return base.plusSeconds(60).toInstant();
    }
    return next.toInstant();
  }
}

