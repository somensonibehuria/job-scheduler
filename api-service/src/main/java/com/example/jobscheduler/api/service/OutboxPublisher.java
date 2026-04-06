package com.example.jobscheduler.api.service;

import java.util.UUID;

import com.example.jobscheduler.api.db.OutboxRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {
  private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

  private final OutboxRepository outbox;
  private final KafkaTemplate<String, String> kafka;
  private final String lockOwner = "api-" + UUID.randomUUID();

  public OutboxPublisher(OutboxRepository outbox, KafkaTemplate<String, String> kafka) {
    this.outbox = outbox;
    this.kafka = kafka;
  }

  @Scheduled(fixedDelayString = "${outbox.publish.delay-ms:100}")
  public void publish() {
    var batch = outbox.claimBatch(lockOwner, 200);
    for (var row : batch) {
      try {
        kafka.send(row.topic(), row.key(), row.payloadJson());
        outbox.markPublished(row.id());
      } catch (Exception e) {
        log.warn("outbox publish failed id={} topic={}", row.id(), row.topic(), e);
      }
    }
  }
}

