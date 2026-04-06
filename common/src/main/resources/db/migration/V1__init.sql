CREATE TABLE IF NOT EXISTS job_definition (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  handler TEXT NOT NULL,
  default_max_attempts INT NOT NULL DEFAULT 5,
  default_timeout_ms INT NOT NULL DEFAULT 30000,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cron_schedule (
  id UUID PRIMARY KEY,
  job_definition_id UUID NOT NULL REFERENCES job_definition(id),
  cron_expr TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  next_fire_at TIMESTAMPTZ NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  version BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cron_schedule_next_fire_at ON cron_schedule (next_fire_at);

CREATE TABLE IF NOT EXISTS job_execution (
  id UUID PRIMARY KEY,
  job_definition_id UUID NOT NULL REFERENCES job_definition(id),
  execution_type TEXT NOT NULL,
  status TEXT NOT NULL,
  scheduled_for TIMESTAMPTZ NOT NULL,
  attempt INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  timeout_ms INT NOT NULL DEFAULT 30000,
  payload JSONB,
  lease_owner TEXT,
  lease_until TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_job_execution_status_scheduled_for ON job_execution (status, scheduled_for);
CREATE INDEX IF NOT EXISTS idx_job_execution_lease_until ON job_execution (lease_until);

CREATE TABLE IF NOT EXISTS job_event (
  id BIGSERIAL PRIMARY KEY,
  execution_id UUID NOT NULL REFERENCES job_execution(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_type TEXT NOT NULL,
  details JSONB
);
CREATE INDEX IF NOT EXISTS idx_job_event_execution_id_ts ON job_event (execution_id, ts);
CREATE INDEX IF NOT EXISTS idx_job_event_ts ON job_event (ts);

CREATE TABLE IF NOT EXISTS outbox (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  message_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  locked_at TIMESTAMPTZ,
  lock_owner TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (published_at, created_at);

