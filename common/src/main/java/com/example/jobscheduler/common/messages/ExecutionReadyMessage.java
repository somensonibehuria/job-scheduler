package com.example.jobscheduler.common.messages;

import java.util.UUID;

public record ExecutionReadyMessage(UUID executionId) {}

