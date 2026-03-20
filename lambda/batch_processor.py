"""
batch_processor.py
──────────────────
SQS-triggered Lambda function.

Workflow per invocation:
  1.  Receive up to 100 SQS messages (each is an S3:ObjectCreated notification)
  2.  Parse raw meter file from S3
  3.  Validate and normalise each reading (timestamp → UTC, units, schema)
  4.  Deduplicate within the batch using (device_id, reading_ts) composite key
  5.  Bulk-insert into Aurora using a single executemany() with ON CONFLICT DO NOTHING
  6.  Report individual message failures so SQS retries only those records
      (not the whole batch) — requires function_response_types = ["ReportBatchItemFailures"]

Environment variables (set by Terraform):
  DB_SECRET_ARN   – Secrets Manager ARN for Aurora credentials
  DB_HOST         – Aurora writer endpoint
  DB_NAME         – Database name
  PROCESSED_BUCKET – S3 bucket for Parquet output
  LOG_LEVEL       – DEBUG (dev) or WARNING (prod)
"""

import json
import logging
import os
import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError
from psycopg2.extensions import connection


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


class InvalidArgumentType(ValueError):
    """
    Raised when an argument is of an unexpected type, such as a non-string timestamp or non-numeric reading value.
    
    """
    pass


# Cache a database connection at the module level to reuse across warm Lambda invocations.
# Reconnect only if the connection is closed or invalid.
_db_conn = None

def _load_credentials() -> Dict[str, str]:
    """
    This function loads all the environment variables required for database connection and other configurations.
    It checks for the presence of each required variable and raises an exception if any are missing.
    This ensures that the Lambda function has all necessary configurations before attempting to connect to the database or process data.

    Args:
        None
    
    Returns:
        Dict[str,str]: A dictionary containing all the loaded environment variables.
    
    Example:
        >>> _load_credentials()
        {
            "DB_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:aurora-credentials",
            "DB_HOST": "aurora-cluster.cluster-123456789012.us-east-1.rds.amazonaws.com",
            "DB_NAME": "mydatabase",
            "PROCESSED_BUCKET": "my-bucket-processed"
        }

    """

    try: 
        sm_client  = boto3.client("secretsmanager")
        credentials = {
            "DB_SECRET_ARN": os.environ["DB_SECRET_ARN"],
            "DB_HOST": os.environ["DB_HOST"],
            "DB_NAME": os.environ["DB_NAME"],
            "PROCESSED_BUCKET": os.environ["PROCESSED_BUCKET"]
        }
        secret_id = credentials["DB_SECRET_ARN"]
        get_secrets = sm_client.get_secret_value(SecretId=secret_id)
        secret = json.loads(get_secrets["SecretString"])
        credentials.update({
            "DB_USERNAME": secret["username"],
            "DB_PASSWORD": secret["password"],
            "DB_PORT": secret.get("port", 5432)
        })
        logger.info({
            "status": "success",
            "message": "Loaded environment variables and secrets successfully"
        })
        return credentials
    
    except KeyError as e:
        missing_var = e.args[0]
        logger.error({
            "status": "error",
            "message": f"Missing required environment variable: {missing_var}",
            "error": str(e)
        })
        raise Exception({
            "status": "error",
            "message": f"Missing required environment variable: {missing_var}",
            "error": str(e)
        })
    except Exception as e:
        logger.error({
            "status": "error",
            "message": "An unexpected error occurred while loading environment variables.",
            "error": str(e)
        })
        raise Exception({
            "status": "error",
            "message": "An unexpected error occurred while loading environment variables.",
            "error": str(e)
        })



def _get_db_connection() -> connection:
    """
    This function implements a lazy-initialized, module-level connection cache
    intended for AWS Lambda execution environments. On warm invocations, the
    previously established connection is reused to avoid the overhead of
    repeatedly opening new database connections.

    A new connection is created when:
      - No connection has been initialized yet
      - The existing connection is closed
      - The connection is no longer valid (e.g. network drop, idle timeout)

    Connection credentials are retrieved from AWS Secrets Manager at the time
    of (re)connection to ensure up-to-date authentication.

    Args:
        None

    Returns:
        connection: An active PostgreSQL connection.

    Raises:
        OperationalError: If a new connection cannot be established.

    Notes:
        - The connection is not thread-safe and assumes a single-threaded
          execution model (e.g. AWS Lambda).
        - Callers are responsible for managing transactions (autocommit is disabled).
        - This pattern relies on Lambda container reuse and is not guaranteed
          across cold starts.
    """
    global _db_conn
    try:
        if _db_conn is None or _db_conn.closed:
            raise OperationalError("No connection")
        _db_conn.isolation_level  #liveness check
    except OperationalError as op_err:
        if "no connection" in str(op_err).lower():
            logger.info("Opening new database connection")
            secrets = _load_credentials()
            _db_conn = psycopg2.connect(
                host=secrets["DB_HOST"],
                port=secrets.get("DB_PORT", 5432),
                dbname=secrets["DB_NAME"],
                user=secrets["DB_USERNAME"],
                password=secrets["DB_PASSWORD"],
                sslmode="require",   # enforced TLS in transit
                connect_timeout=10,
                # Kept-alive so the connection survives Lambda idle time
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            _db_conn.autocommit = False
            logger.info({
                "status": "success",
                "message": "Database connection established successfully"
            })
        else:
            logger.error({
                "status": "error",
                "message": "Failed to connect to database",
                "error": str(op_err)
            })
            raise Exception({
                "status": "error",
                "message": "Failed to connect to database",
                "error": str(op_err)
            })
    
    return _db_conn



def _normalise_timestamp(raw_ts: str) -> datetime:
    """
    This converts an arbitrary timestamp string to a UTC-aware datetime.

    Meters transmit timestamps in various formats and local time zones.
    We normalise everything to UTC before storage so that queries and
    joins across sites with different physical locations produce correct results.

    Handles:
      - ISO 8601 with offset   e.g. 2024-05-01T10:30:00+01:00
      - ISO 8601 UTC           e.g. 2024-05-01T09:30:00Z
      - Unix epoch (seconds)   e.g. 1714556400
      - Space-separated local  e.g. 2024-05-01 10:30:00  (assumed UTC if no TZ)
    
    Args:
        raw_ts (str): The raw timestamp string from the meter reading.

    Returns:
        datetime: A timezone-aware datetime object in UTC.
    """
    try:
        if not isinstance(raw_ts, str):
            raise InvalidArgumentType(f"Expected timestamp as string, got {type(raw_ts).__name__}")
        
        raw_ts = raw_ts.strip()

        # Unix epoch
        if raw_ts.isdigit():
            return datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)

        # Normalise Z suffix for fromisoformat() (Python <3.11 doesn't accept Z)
        raw_ts = raw_ts.replace("Z", "+00:00").replace(" ", "T")

        dt = datetime.fromisoformat(raw_ts)
        if dt.tzinfo is None:
            # Assume UTC if no timezone info present
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        logger.info({
            "status": "success",
            "message": f"Normalised timestamp: {raw_ts} → {dt.isoformat()}"
        })
        return dt

    except Exception as e:
        logger.error(
            {
                "status": "error",
                "message": f"Failed to normalise timestamp: {raw_ts}",
                "error": str(e)
            }
        )
        raise Exception(
            {
                "status": "error",
                "message": f"Failed to normalise timestamp: {raw_ts}",
                "error": str(e)
            }
        )



def _parse_meter_file(bucket: str, key: str) -> list[dict]:
    """
    Download and parse a raw meter CSV/JSON file from S3.

    Expected CSV columns:
      device_id, site_id, reading_ts, voltage_a, voltage_b, voltage_c,
      current_a, current_b, current_c, power_factor, frequency_hz
    """
    try:
        if not isinstance(bucket, str):
            raise InvalidArgumentType(f"Expected bucket name as string, got {type(bucket).__name__}")
        if not isinstance(key, str):
            raise InvalidArgumentType(f"Expected key name as string, got {type(key).__name__}")
        
        logger.debug("Parsing s3://%s/%s", bucket, key)
        
        s3_client  = boto3.client("s3")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")

        rows = []
        reader = csv.DictReader(io.StringIO(body))
        for i, row in enumerate(reader):
            try:
                rows.append({
                    "device_id":   row["device_id"].strip(),
                    "site_id":     row["site_id"].strip(),
                    "reading_ts":  _normalise_timestamp(row["reading_ts"]),
                    "voltage_a":   float(row["voltage_a"]),
                    "voltage_b":   float(row["voltage_b"]),
                    "voltage_c":   float(row["voltage_c"]),
                    "current_a":   float(row["current_a"]),
                    "current_b":   float(row["current_b"]),
                    "current_c":   float(row["current_c"]),
                    "power_factor": float(row["power_factor"]),
                    "frequency_hz": float(row["frequency_hz"]),
                    "ingested_at": datetime.now(tz=timezone.utc),
                    "source_key":  key,   # preserve provenance for debugging
                })
            except (KeyError, ValueError) as exc:
                # Log and skip malformed rows rather than failing the whole file
                logger.warning("Skipping row %d in %s: %s | row=%s", i, key, exc, row)
        logger.info({
            "status": "success",
            "message": f"Parsed {len(rows)} valid readings from s3://{bucket}/{key}",
        })
        return rows
    
    except Exception as e:
        logger.error({
                "status": "error",
                "message": f"Failed to parse meter file s3://{bucket}/{key}",
                "error": str(e)            
            })
        raise Exception({
            "status": "error",
            "message": f"Failed to parse meter file s3://{bucket}/{key}",
            "error": str(e)
        })
        



def _deduplicate(readings: list[dict]) -> list[dict]:
    """
    This removes duplicate readings within a single Lambda invocation batch.

    The composite key is (device_id, reading_ts) – the same key used in
    the database UNIQUE constraint.  Deduplication here reduces unnecessary
    DB round-trips; the ON CONFLICT clause in the INSERT catches any
    duplicates that slip through across invocations or from late-arriving data.

    Strategy: last-write-wins within a batch (later entries overwrite earlier
    ones for the same key, which is safe because readings for a given moment
    are idempotent – the value doesn't change).

    Args:
        readings (list[dict]): A list of meter reading dictionaries to deduplicate.

    Returns:
        list[dict]: A deduplicated list of meter reading dictionaries.
    
    Example:
        >>> readings = [
                {"device_id": "dev1", "reading_ts": datetime(2024, 5, 1, 10, 0), "value": 100},
                {"device_id": "dev1", "reading_ts": datetime(2024, 5, 1, 10, 0), "value": 110},  # duplicate timestamp
                {"device_id": "dev2", "reading_ts": datetime(2024, 5, 1, 10, 5), "value": 200},
            ]
        >>> _deduplicate(readings)
        [
            {"device_id": "dev1", "reading_ts": datetime(2024, 5, 1, 10, 0), "value": 110},  # last write wins
            {"device_id": "dev2", "reading_ts": datetime(2024, 5, 1, 10, 5), "value": 200},
        ]

    """
    try:
        if not isinstance(readings, list):  
            raise InvalidArgumentType(f"Expected readings as a list of dicts, got {type(readings).__name__}")
        
        seen: dict[tuple, dict] = {}
        for r in readings:
            key = (r["device_id"], r["reading_ts"])
            seen[key] = r  # newer entry overwrites older for the same key
        
        logger.info({
            "status": "success",
            "message": f"Deduplicated {len(readings)} readings to {len(seen)} unique entries"
        })
        return list(seen.values())
    
    except Exception as e:
        logger.error({
            "status": "error",
            "message": "Failed to deduplicate readings",
            "error": str(e)
        })
        raise Exception({
            "status": "error",
            "message": "Failed to deduplicate readings",
            "error": str(e)
        })
    


def _batch_insert(conn, readings: list[dict]) -> int:
    """
    This inserts all readings in a single parameterised batch statement.

    ON CONFLICT DO NOTHING ensures:
      - Exact duplicates (same device_id + reading_ts) are silently skipped
      - The raw file is never lost (S3 versioning preserves it)
      - Re-processing a file after a failure is safe (idempotent)

    Returns the number of rows actually inserted (excluding skipped duplicates).

    Args:
        conn: An active database connection.
        readings (list[dict]): A list of meter reading dictionaries to insert into the database.

    Returns:
        int: The number of rows successfully inserted into the database, excluding duplicates.

    Example:
        >>> conn = _get_db_connection()
        >>> readings = [
                {"device_id": "dev1", "reading_ts": datetime(2024, 5, 1, 10, 0), "value": 100},
                {"device_id": "dev1", "reading_ts": datetime(2024, 5, 1, 10, 0), "value": 110},  # duplicate timestamp
                {"device_id": "dev2", "reading_ts": datetime(2024, 5, 1, 10, 5), "value": 200},
            ]
        >>> _batch_insert(conn, readings)
        2  # one row skipped due to duplicate key

    """
    try:
        if not isinstance(conn, connection):
            raise InvalidArgumentType(f"Expected a psycopg2 connection object, got {type(conn).__name__}")
        
        if not isinstance(readings, list):
            raise InvalidArgumentType(f"Expected readings as a list of dicts, got {type(readings).__name__}")
        
        if not readings:
            return 0

        insert_sql = """
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
            execute_values(cur, insert_sql, values, page_size=500)
            inserted = cur.rowcount  # -1 if driver doesn't expose it; treated as success
        
        conn.commit()
        
        logger.info({
            "status": "success",
            "message": f"Inserted {inserted} rows (of {len(readings)} attempted)"
        })
        return max(inserted, 0)
    
    except Exception as e:
        conn.rollback()      
        global _db_conn
        _db_conn = None
        logger.error({
            "status": "error",
            "message": "Failed to insert readings into database",
            "error": str(e)
        })
        raise Exception({
            "status": "error",
            "message": "Failed to insert readings into database",
            "error": str(e)
        })

    

def handler(event: dict, context: Any) -> dict:
    """
    This is Lambda entry point.

    SQS batch with ReportBatchItemFailures:
      - Return {"batchItemFailures": [...]} to tell SQS which message IDs failed
      - Successfully processed messages are automatically deleted by SQS
      - Failed messages are retried (up to maxReceiveCount) then sent to DLQ
    
    Args:
        event (dict): The event payload from SQS, containing a list of messages.
        context: The Lambda execution context (not used here).

    Returns:
        dict: A dictionary with a "batchItemFailures" key listing failed message IDs for SQS to retry.

    example:
    {
        "batchItemFailures": [
            {"itemIdentifier": "message-id-1"},
            {"itemIdentifier": "message-id-3"}
        ]
    }
    """
    batch_item_failures = []
    all_readings: list[dict] = []

    # Step 1: Parse all S3 files referenced in this SQS batch
    message_readings: dict[str, list[dict]] = {}  # message_id → readings

    for message in event.get("Records", []):
        message_id = message["messageId"]
        try:
            body = json.loads(message["body"])
            s3_event = body.get("Records", [])

            readings_for_message = []
            for s3_record in s3_event:
                bucket = s3_record["s3"]["bucket"]["name"]
                key    = s3_record["s3"]["object"]["key"]
                readings_for_message.extend(_parse_meter_file(bucket, key))

            message_readings[message_id] = readings_for_message
            all_readings.extend(readings_for_message)

        except Exception as e:
            logger.error({
                "status": "error",
                "message": f"Failed to parse message {message_id}",
                "error": str(e)
            })
            batch_item_failures.append({"itemIdentifier": message_id})

    if not all_readings:
        logger.info({
            "status": "info",
            "message": "No valid readings parsed from any messages in the batch, returning early"
        })
        return {"batchItemFailures": batch_item_failures}

    # Step 2: Deduplicate across the entire batch
    deduped = _deduplicate(all_readings)
    logger.info({
        "status": "error",
        "message": f"Batched: {len(all_readings)} raw readings -> {len(deduped)} after dedup"
    })

    # Step 3: Bulk-insert into Aurora 
    try:
        conn = _get_db_connection()
        _batch_insert(conn, deduped)

    except Exception as e:
        # DB failure – resets the cached connection so the next warm invocation
        # doesn't try to reuse a dead socket, then it marks every message for retry.
        # ON CONFLICT DO NOTHING makes re-processing safe.
        global _db_conn
        _db_conn = None
        logger.error({
            "status": "error",
            "message": "DB insertion failed, marking all messages for retry",
            "error": str(e)
        })
        all_ids = [m["messageId"] for m in event.get("Records", [])]
        return {
            "batchItemFailures": [{"itemIdentifier": mid} for mid in all_ids]
        }

    return {"batchItemFailures": batch_item_failures}

