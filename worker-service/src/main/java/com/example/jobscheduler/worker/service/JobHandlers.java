package com.example.jobscheduler.worker.service;

import java.util.Map;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class JobHandlers {
  private static final Logger log = LoggerFactory.getLogger(JobHandlers.class);

  private final Map<String, Consumer<JsonNode>> handlers = Map.of(
      "sampleCron", payload -> log.info("sampleCron executed payload={}", payload),
      "sampleAdhoc", payload -> log.info("sampleAdhoc executed payload={}", payload)
  );

  public void run(String handler, JsonNode payload) {
    Consumer<JsonNode> fn = handlers.get(handler);
    if (fn == null) {
      throw new IllegalArgumentException("Unknown handler: " + handler);
    }
    fn.accept(payload);
  }
}

