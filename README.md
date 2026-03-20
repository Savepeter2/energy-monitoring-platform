# Smarterise IoT Energy Platform – Solution Framework

**Version:** 1.0  
**Author:** Data Engineering Team  
**Audience:** Engineers, architects, and on-call responders  

---

## 1. How the Solution Works End to End

The pipeline has two ingest paths that converge at a single Aurora PostgreSQL writer.

### 1.1 FTP / File Path

1. Smart meters at transformer sites deposit structured CSV payloads to **AWS Transfer Family**, a managed SFTP endpoint. This replaces any self-hosted FTP server and eliminates an EC2 instance to maintain.
2. Transfer Family writes files directly into the **S3 Raw bucket** under the `raw/` prefix.
3. S3 emits an `ObjectCreated` event notification to an **SQS queue**. SQS acts as a durable buffer — if Lambda is throttled or down, messages wait rather than being dropped.
4. The **batch Lambda** function is triggered by SQS. Each invocation receives up to 100 messages (each pointing to one S3 file), reads the files from S3, parses and validates all rows, deduplicates on `(device_id, reading_ts)`, and performs a single bulk `INSERT … ON CONFLICT DO NOTHING` into Aurora.
5. Failed records are reported individually back to SQS via `ReportBatchItemFailures`. Only failed messages are retried; successful ones are auto-deleted. After three failures a message moves to the **Dead-Letter Queue (DLQ)** for investigation without being silently dropped.

### 1.2 MQTT / Streaming Path

1. Meters publish JSON payloads via MQTT to **AWS IoT Core**, which applies rules and routes messages to a **Kinesis Data Stream**.
2. The **stream Lambda** function consumes from Kinesis with `TRIM_HORIZON` starting position (ensures no records are missed). Each invocation processes a shard batch of up to 100 records.
3. The same normalisation, deduplication, and bulk-insert pattern as the batch path is applied.
4. `bisect_batch_on_function_error = true` means Lambda automatically splits a failing batch in half to isolate any single malformed record without blocking the entire shard.

### 1.3 Storage and Serving

- **Aurora Serverless v2** hosts the `meter_readings` table, partitioned by month. A separate **read replica** handles all dashboard and web app queries, protecting the writer from read load.
- **ElastiCache (Redis)** caches frequently repeated dashboard query results (e.g. hourly aggregates) with a short TTL, further reducing Aurora read replica pressure.
- **QuickSight** connects to the Aurora reader endpoint via a private VPC connection.
- The **customer-facing web app** (ECS Fargate) queries via the reader endpoint or through the Redis cache.

---

## 2. AWS Service Justifications

| Service | Role | Why this service |
|---|---|---|
| **AWS Transfer Family** | SFTP endpoint for meter file drops | Fully managed; no EC2 to patch; native IAM integration; writes directly to S3 |
| **S3** | Raw + processed storage | Infinitely scalable; per-object encryption; lifecycle policies for automatic cost tiering |
| **SQS** | Buffer between S3 events and Lambda | Decouples ingestion bursts from processing; provides durable retry with DLQ; no data loss on Lambda throttle |
| **Kinesis Data Stream** | MQTT streaming path | Ordered, sharded delivery; replay window for recovery; scales by adding shards |
| **AWS IoT Core** | MQTT broker | Managed broker at scale; handles TLS, auth, and rule-based routing without custom broker infrastructure |
| **Lambda** | Transform compute | No servers to manage; scales to concurrency automatically; reserved concurrency caps DB connection count |
| **Aurora Serverless v2** | Primary database | Scales ACUs in seconds; writer + reader split; PostgreSQL-compatible; native Secrets Manager rotation |
| **ElastiCache (Redis)** | Query caching | Sub-millisecond reads; reduces Aurora load for repeated dashboard queries |
| **Secrets Manager** | Credential management | Auto-rotation; fine-grained IAM access; eliminates hardcoded credentials |
| **VPC + Private Subnets** | Network isolation | Lambda and Aurora have no internet route; attack surface is minimal |
| **CloudWatch** | Observability | Native metrics for all services; custom alarms; log retention management |
| **ECS Fargate** | Web app hosting | Serverless containers; no EC2 fleet to manage; scales independently of the pipeline |

---

## 3. Duplicate Records and Exactly-Once Processing

### 3.1 The Guarantee

The pipeline provides **at-least-once delivery with idempotent writes**, which is operationally equivalent to exactly-once semantics for this use case.

### 3.2 How Duplicates Are Handled

**Step 1 — Intra-batch deduplication (in Lambda memory)**  
Before touching the database, the Lambda function builds a dictionary keyed on `(device_id, reading_ts)`. If the same key appears multiple times in one batch (e.g. a meter sends a retry), only the last-seen entry is kept. This eliminates the majority of duplicates at zero database cost.

**Step 2 — Database constraint (across invocations)**  
The `meter_readings` table has a composite primary key on `(device_id, reading_ts)`. Every INSERT uses `ON CONFLICT (device_id, reading_ts) DO NOTHING`. This means:
- Re-processing the same S3 file (e.g. after a Lambda failure and retry) is safe — existing rows are skipped without error.
- Late-arriving MQTT duplicates of already-inserted FTP readings are silently skipped.
- No raw data is lost: the original file remains in S3 with versioning.

**Step 3 — SQS message visibility / Kinesis checkpointing**  
SQS does not delete a message until Lambda explicitly acknowledges it (via not returning it in `batchItemFailures`). Kinesis advances the shard iterator only after a successful batch. If Lambda crashes mid-insert, the message/records are re-delivered and the idempotent INSERT handles the overlap.

---

## 4. Timestamp Standardisation

All timestamps are normalised to **UTC-aware datetimes before the database INSERT**. The `_normalise_timestamp()` function handles:

| Input format | Example | Action |
|---|---|---|
| Unix epoch (seconds) | `1714556400` | `datetime.fromtimestamp(..., tz=timezone.utc)` |
| ISO 8601 with offset | `2024-05-01T10:30:00+01:00` | `fromisoformat()` → `.astimezone(UTC)` |
| ISO 8601 UTC (`Z`) | `2024-05-01T09:30:00Z` | Replace `Z` with `+00:00` → `fromisoformat()` |
| Space-separated local | `2024-05-01 10:30:00` | Replace space with `T`, assume UTC if no tzinfo |

The Aurora column `reading_ts` is defined as `TIMESTAMPTZ` (timestamp with time zone), which PostgreSQL always stores internally as UTC. This ensures that joins across sites in different time zones (e.g. multiple African countries in a future expansion) produce correct results without any time-zone conversion logic in the query layer.

---

## 5. Security Measures

### 5.1 Data Encryption

| Layer | Mechanism |
|---|---|
| S3 at rest | AES-256 (SSE-S3); `bucket_key_enabled` reduces API cost if upgrading to KMS |
| Kinesis at rest | KMS encryption using `alias/aws/kinesis` |
| SQS at rest | KMS encryption using `alias/aws/sqs` |
| Aurora at rest | `storage_encrypted = true` (AWS-managed key) |
| Data in transit | TLS enforced for all connections; Lambda psycopg2 uses `sslmode="require"`; Transfer Family uses SFTP (SSH over TLS) |

### 5.2 Network Isolation

Lambda functions and Aurora run in **private subnets with no internet gateway route**. Traffic to AWS services (S3, Secrets Manager, Kinesis) flows through **VPC Interface Endpoints** — it never leaves the AWS backbone and incurs no NAT data-processing charges.

### 5.3 IAM Least Privilege

Each Lambda has its own IAM role. Permissions are scoped to specific resource ARNs:
- Batch Lambda: read only `raw/` prefix in S3, write only the `processed/` bucket, consume the SQS queue, read the specific Secrets Manager secret.
- Stream Lambda: read only the specific Kinesis stream, read the specific Secrets Manager secret.
- No Lambda role has `*` resource except the EC2 VPC networking actions, which AWS requires as `*` by service design.

### 5.4 Secrets Management

Database credentials are stored in Secrets Manager and fetched at Lambda cold-start. The secret ARN (not the value) is passed via environment variable. Credentials are cached in the Lambda execution environment memory for subsequent warm invocations, reducing API call volume. Automatic rotation is enabled on a 30-day schedule.

---

## 6. Cost Optimisation and Performance Under Load

### 6.1 Cost Controls

| Mechanism | Saving |
|---|---|
| S3 lifecycle transitions | Raw files move from Standard → IA → Glacier → expiry; up to 85 % cost reduction on storage after 90 days |
| Aurora Serverless v2 | Scales to 0.5 ACU at night (near-zero cost); only scales up during processing bursts |
| ElastiCache | Repeated dashboard queries hit Redis (sub-millisecond, fixed cost) rather than Aurora (per-ACU-hour) |
| VPC Endpoints | Eliminate NAT Gateway data-processing charges for S3, Kinesis, Secrets Manager traffic |
| SQS batching | 100 messages per Lambda invocation vs. 1 = 100× fewer invocations = 100× fewer billable Lambda GB-seconds |
| Reserved concurrency | Caps Lambda at 50 concurrent for batch; prevents over-scaling that would exhaust Aurora connections |

### 6.2 Performance Under Increased Load

- **Adding sites**: no structural changes required. More meters → more SQS messages → Lambda scales invocation concurrency automatically. Add Kinesis shards to increase MQTT throughput (one `terraform apply` with updated `shard_count`).
- **Aurora connection exhaustion** (the current bottleneck): solved by the batching approach (Section 7) + connection caching in Lambda warm starts. Reserved concurrency acts as the upper bound.
- **Dashboard query performance**: the read replica is isolated from write traffic. The `hourly_site_aggregates` materialised view pre-aggregates data so QuickSight queries scan the view (one row per site per hour) rather than the raw table (one row per reading every few minutes).

---

## 7. On-Call Runbook: Ingestion Pipeline Stops Writing to the Database

Follow these steps in order when alerted that no new data is reaching Aurora.

### Step 1 — Identify which path has failed (5 min)

Check CloudWatch dashboards:

```
# SQS path metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/SQS \
  --metric-name NumberOfMessagesSent \
  --dimensions Name=QueueName,Value=smarterise-prod-meter-queue \
  --start-time $(date -u -v-30M +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 --statistics Sum

# Lambda errors
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=smarterise-prod-batch \
  --start-time $(date -u -v-30M +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 --statistics Sum
```

### Step 2 — Check Lambda logs (5 min)

```bash
# Tail recent Lambda errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/smarterise-prod-batch \
  --filter-pattern "ERROR" \
  --start-time $(date +%s000 -d "30 minutes ago")
```

**Common error signatures:**

| Log message | Likely cause | Action |
|---|---|---|
| `connection refused` / `could not connect` | Aurora scaled to 0 or VPC routing issue | Check Aurora cluster status; check security group rules |
| `SSL SYSCALL error` | TLS handshake failure | Check security group; verify `sslmode=require` in Lambda env |
| `password authentication failed` | Secret rotation changed password, Lambda has stale cache | Force Lambda cold start by updating an env var; verify secret version in Secrets Manager |
| `Too many connections` | Aurora connection limit hit | Reduce `reserved_concurrent_executions`; consider RDS Proxy |
| `S3 Access Denied` | IAM policy issue or S3 bucket policy changed | Check IAM role policy; check S3 bucket policy |
| `JSONDecodeError` / `KeyError` | Schema change from meters | Check raw file format in S3; update parser if meter firmware changed |

### Step 3 — Check DLQ depth

```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.eu-west-1.amazonaws.com/<ACCOUNT>/smarterise-prod-meter-dlq \
  --attribute-names ApproximateNumberOfMessages
```

If DLQ has messages, download one to inspect the failed payload:

```bash
aws sqs receive-message \
  --queue-url https://sqs.eu-west-1.amazonaws.com/<ACCOUNT>/smarterise-prod-meter-dlq
```

### Step 4 — Check Aurora cluster health

```bash
aws rds describe-db-clusters \
  --db-cluster-identifier smarterise-prod-aurora \
  --query 'DBClusters[0].{Status:Status,Capacity:ServerlessV2ScalingConfiguration}'
```

If Aurora is in `failing-over` state, wait for the automatic failover to the reader (typically 30–60 seconds). No action required.

### Step 5 — Replay failed messages from DLQ

Once the root cause is fixed, replay DLQ messages back to the main queue:

```bash
# Redrive all DLQ messages back to the processing queue
aws sqs start-message-move-task \
  --source-arn arn:aws:sqs:eu-west-1:<ACCOUNT>:smarterise-prod-meter-dlq \
  --destination-arn arn:aws:sqs:eu-west-1:<ACCOUNT>:smarterise-prod-meter-queue
```

Files that failed will be reprocessed. The `ON CONFLICT DO NOTHING` constraint ensures no duplicates are created for files that were partially processed before the failure.

### Step 6 — Confirm recovery

```sql
-- Run against Aurora reader endpoint
SELECT
    DATE_TRUNC('minute', ingested_at) AS minute,
    COUNT(*) AS rows_inserted
FROM meter_readings
WHERE ingested_at > NOW() - INTERVAL '30 minutes'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 15;
```

Rows should appear with timestamps matching the current time.

---

## 8. Bonus: Data Batching Strategy

### 8.1 Problem Statement

The original pipeline called `execute()` once per row in a loop. Each call opens and closes a cursor, incurs a network round-trip, and holds a connection for the duration. With hundreds of active sites transmitting every few minutes, this causes:
- Connection pool exhaustion (PostgreSQL has a hard connection limit)
- High cumulative latency (N rows × N round-trips)
- Lambda timeout risk on large files

### 8.2 Determining Appropriate Batch Size

The batch size is determined by three constraints:

**Memory constraint:** Lambda is allocated 512 MB. A typical meter reading object in Python takes approximately 800 bytes. At a batch size of 500 rows that is 400 KB — well within the limit.

**Database constraint:** Aurora's `max_locks_per_transaction` and executor memory limit practical INSERT sizes to around 500–1000 rows per statement. `execute_values` with `page_size=500` splits larger lists automatically.

**SQS constraint:** Each SQS message references one S3 file. Files typically contain 100–500 readings (a few minutes of data per meter). With `batch_size=100` SQS messages per Lambda invocation, a single invocation can process up to 50,000 readings — all in one `execute_values` call.

**Recommended setting:** `page_size=500` rows per `execute_values` page. Adjust down to 200 if you observe `out of memory` errors in Lambda logs.

### 8.3 Deduplication with the Composite Key

The composite key `(device_id, reading_ts)` is the deduplication unit:

```python
# In-memory dedup before the INSERT
seen: dict[tuple, dict] = {}
for reading in all_readings:
    key = (reading["device_id"], reading["reading_ts"])
    seen[key] = reading   # last-write-wins within the batch

deduped_readings = list(seen.values())
```

Then in the database:

```sql
INSERT INTO meter_readings (...) VALUES %s
ON CONFLICT (device_id, reading_ts) DO NOTHING;
```

This two-layer approach means:
- Layer 1 (Python dict): eliminates duplicates within a single Lambda invocation, reducing INSERT size and Aurora write amplification.
- Layer 2 (SQL UNIQUE): handles duplicates across invocations — e.g. SQS delivers the same message twice, or the FTP and MQTT paths both deliver the same reading.

### 8.4 Handling Partial Batch Failure Without Data Loss or Duplication

The key concern: if `execute_values` partially succeeds (e.g. inserts rows 1–250 then the connection drops before rows 251–500), how do we avoid re-inserting 1–250?

**Answer:** `ON CONFLICT DO NOTHING` makes the insert idempotent. When the Lambda function retries (triggered by SQS re-delivery), it re-inserts all rows. Rows 1–250 hit the conflict clause and are skipped. Rows 251–500 are inserted. The end state is correct.

The complete transaction pattern:

```python
try:
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=500)
    conn.commit()               # Commits the entire batch atomically
except Exception:
    conn.rollback()             # Roll back any partial writes
    raise                       # Re-raise so Lambda marks the message as failed
                                # SQS will re-deliver; ON CONFLICT handles overlaps
```

Key points:
- The entire `execute_values` call is wrapped in a single transaction.
- If the transaction rolls back, nothing is written — so there is no partial state to worry about.
- On retry, the full batch is attempted again. Rows that were in a previous successful commit are skipped by `ON CONFLICT`.
- This means the only scenario requiring special handling is a commit that partially succeeds at the PostgreSQL wire level — which does not happen in PostgreSQL's transactional model. A transaction either commits fully or not at all.

### 8.5 Connection Pool Management

Each Lambda function caches the `psycopg2` connection as a module-level variable. On a warm invocation, no new TCP connection is opened. With `reserved_concurrent_executions = 50`, the maximum simultaneous Aurora connections from Lambda is 50. Aurora Serverless v2 with `max_capacity = 16 ACU` supports approximately 180 connections at max scale — well above the Lambda cap.

For future scaling beyond 50 concurrent Lambdas, add **RDS Proxy** in front of Aurora. RDS Proxy multiplexes Lambda connections and pins connection count to a configurable pool size regardless of Lambda concurrency.

---

## 9. Schema Evolution and Operational Notes

### Adding a new site / meter

1. Insert a row into `site_meter_map` with the new `meter_id` and `site_id`.
2. No Lambda code change required — site-to-meter mapping is now in the database.

### Scaling to 500+ sites

- Increase `kinesis_shard_count` in `terraform.tfvars` and apply.
- Increase `reserved_concurrent_executions` on the batch Lambda (one per ~10 concurrent SQS polls).
- Add Aurora read replicas if dashboard query latency increases.
- Consider partitioning by both `site_id` and `reading_ts` if single-site query performance degrades.

### Partition maintenance

Use `pg_partman` to automatically create future monthly partitions:

```sql
CREATE EXTENSION IF NOT EXISTS pg_partman;
SELECT partman.create_parent(
    p_parent_table => 'public.meter_readings',
    p_control => 'reading_ts',
    p_type => 'range',
    p_interval => '1 month',
    p_premake => 3  -- create 3 months of future partitions in advance
);
```

### Refreshing the materialised view

Schedule this via EventBridge → Lambda or `pg_cron`:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_site_aggregates;
```

`CONCURRENTLY` allows reads to continue without locking during the refresh.
