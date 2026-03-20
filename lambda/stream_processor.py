"""
stream_processor.py
───────────────────
Kinesis-triggered Lambda function for the MQTT real-time path.

Kinesis delivers records in ordered, per-shard batches.  This function:
  1.  Decodes and parses each Kinesis record (base64-encoded JSON payload)
  2.  Normalises timestamps to UTC
  3.  Deduplicates within the batch
  4.  Batch-inserts into Aurora (same ON CONFLICT safety net as batch path)
  5.  Reports individual item failures so Lambda bisect-on-error can isolate
      poison-pill records without blocking the shard indefinitely

Kinesis ordering guarantee: within a shard, records are processed in
sequence.  Because we write the composite key (device_id, reading_ts),
late-arriving duplicates from the FTP path are handled by ON CONFLICT.
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import psycopg2
from psycopg2.extras import execute_values

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

sm_client = boto3.client("secretsmanager")
_db_conn = None


def _get_db_connection():
    global _db_conn
    try:
        if _db_conn is None or _db_conn.closed:
            raise psycopg2.OperationalError
        _db_conn.isolation_level
    except psycopg2.OperationalError:
        secret = json.loads(
            sm_client.get_secret_value(
                SecretId=os.environ["DB_SECRET_ARN"]
            )["SecretString"]
        )
        _db_conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=secret.get("port", 5432),
            dbname=os.environ["DB_NAME"],
            user=secret["username"],
            password=secret["password"],
            sslmode="require",
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        _db_conn.autocommit = False
    return _db_conn


def _parse_kinesis_record(record: dict) -> dict | None:
    """
    Decode and validate a single Kinesis record.

    Expected payload (JSON):
    {
        "device_id":    "METER_001",
        "site_id":      "SITE_LAG_042",
        "ts":           1714556400,          ← Unix epoch or ISO 8601
        "voltage":      {"a": 230.1, "b": 229.8, "c": 231.4},
        "current":      {"a": 12.3, "b": 11.9, "c": 12.7},
        "power_factor": 0.97,
        "frequency_hz": 50.01
    }
    """
    try:
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        payload = json.loads(raw)

        # Normalise timestamp to UTC
        raw_ts = str(payload["ts"]).strip()
        if raw_ts.isdigit():
            reading_ts = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)
        else:
            raw_ts = raw_ts.replace("Z", "+00:00").replace(" ", "T")
            dt = datetime.fromisoformat(raw_ts)
            reading_ts = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        v = payload["voltage"]
        c = payload["current"]

        return {
            "device_id":    payload["device_id"],
            "site_id":      payload["site_id"],
            "reading_ts":   reading_ts,
            "voltage_a":    float(v["a"]),
            "voltage_b":    float(v["b"]),
            "voltage_c":    float(v["c"]),
            "current_a":    float(c["a"]),
            "current_b":    float(c["b"]),
            "current_c":    float(c["c"]),
            "power_factor": float(payload["power_factor"]),
            "frequency_hz": float(payload["frequency_hz"]),
            "ingested_at":  datetime.now(tz=timezone.utc),
            "source_key":   record["kinesis"]["sequenceNumber"],  # provenance
        }
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Invalid Kinesis record: %s | record=%s", exc, record.get("kinesis", {}).get("sequenceNumber"))
        return None


def _deduplicate(readings: list[dict]) -> list[dict]:
    seen: dict[tuple, dict] = {}
    for r in readings:
        seen[(r["device_id"], r["reading_ts"])] = r
    return list(seen.values())


def _batch_insert(conn, readings: list[dict]) -> None:
    if not readings:
        return
    sql = """
        INSERT INTO meter_readings (
            device_id, site_id, reading_ts,
            voltage_a, voltage_b, voltage_c,
            current_a, current_b, current_c,
            power_factor, frequency_hz,
            ingested_at, source_key
        )
        VALUES %s
        ON CONFLICT (device_id, reading_ts) DO NOTHING
    """
    values = [
        (
            r["device_id"], r["site_id"], r["reading_ts"],
            r["voltage_a"], r["voltage_b"], r["voltage_c"],
            r["current_a"], r["current_b"], r["current_c"],
            r["power_factor"], r["frequency_hz"],
            r["ingested_at"], r["source_key"],
        )
        for r in readings
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    logger.info("Stream insert: %d rows", len(readings))


def handler(event: dict, context: Any) -> dict:
    """
    Kinesis handler with per-record failure reporting.

    bisect_batch_on_function_error = true in Terraform means Lambda will
    split a failing batch in half and retry each half separately, quickly
    isolating any single poison-pill record to its own invocation where it
    then goes to the on_failure destination (SQS DLQ).
    """
    item_failures = []
    all_readings: list[dict] = []
    sequence_map: dict[str, list[int]] = {}  # sequence_number → indices in all_readings

    for record in event.get("Records", []):
        seq = record["kinesis"]["sequenceNumber"]
        parsed = _parse_kinesis_record(record)
        if parsed:
            idx = len(all_readings)
            all_readings.append(parsed)
            sequence_map[seq] = idx
        else:
            # Unparseable record – report as failure so it goes to DLQ
            item_failures.append({"itemIdentifier": seq})

    deduped = _deduplicate(all_readings)

    try:
        conn = _get_db_connection()
        _batch_insert(conn, deduped)
    except Exception as exc:
        logger.error("Stream DB insert failed: %s", exc, exc_info=True)
        # Mark every record as failed – Kinesis will retry (bisect will isolate bad records)
        all_seqs = [r["kinesis"]["sequenceNumber"] for r in event.get("Records", [])]
        return {"batchItemFailures": [{"itemIdentifier": s} for s in all_seqs]}

    return {"batchItemFailures": item_failures}
