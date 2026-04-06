# Job scheduler

Distributed-ready skeleton job scheduler:

- `api-service` (port `8080`): accepts cron/ad-hoc triggers and stores executions
- `scheduler-service` (port `8081`): turns cron schedules + Redis-delayed retries into Kafka work items
- `worker-service` (port `8082`): consumes Kafka, leases executions from Postgres, runs handlers, retries via Redis

Backed by **Kafka + Postgres + Redis**.

## Requirements

- Java 21+
- Maven 3.9+
- Docker (for local stack)

## Start infra

```bash
cd "Job scheduler"
docker compose up -d
```

## Run services (3 terminals)

```bash
cd "Job scheduler"
mvn -pl api-service spring-boot:run
```

```bash
cd "Job scheduler"
mvn -pl scheduler-service spring-boot:run
```

```bash
cd "Job scheduler"
mvn -pl worker-service spring-boot:run
```

## Legacy APIs (simple entry points)

- `POST /api/jobs/cron/run` -> creates a cron execution (published to Kafka via outbox)
- `POST /api/jobs/adhoc/run` -> creates an ad-hoc execution (published to Kafka via outbox)
- `GET /api/jobs/{id}` and `GET /api/jobs`

### Example

```bash
curl -X POST http://localhost:8080/api/jobs/cron/run
curl -X POST http://localhost:8080/api/jobs/adhoc/run \
  -H 'Content-Type: application/json' \
  -d '{"payload":"run-report"}'
curl http://localhost:8080/api/jobs
```

## Notes

- This skeleton uses **at-least-once** processing + worker leasing; make handlers idempotent.
- 1-year retention is implemented as a simple delete job; for production at scale prefer partitioned tables and drop partitions.
