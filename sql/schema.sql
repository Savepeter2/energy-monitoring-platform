-- Smarterise IoT Platform – Aurora PostgreSQL Schema
-- This DDL is ran once after the Aurora cluster is provisioned.


-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_cron";


-- Reference table: site_meter_map
CREATE TABLE IF NOT EXISTS site_meter_map (
    meter_id        TEXT        PRIMARY KEY,
    site_id         TEXT        NOT NULL,
    site_name       TEXT,
    location_lat    NUMERIC(9,6),
    location_lng    NUMERIC(9,6),
    installed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    active          BOOLEAN     NOT NULL DEFAULT TRUE,
    metadata        JSONB
);

CREATE INDEX IF NOT EXISTS idx_site_meter_site_id
    ON site_meter_map (site_id);


-- Core fact table: meter_readings
-- Partitioned by month (RANGE on reading_ts).
-- Partitions are created automatically by create_monthly_partitions() below,
-- scheduled via pg_cron – no manual partition management ever needed.
CREATE TABLE IF NOT EXISTS meter_readings (
    id              BIGSERIAL,
    device_id       TEXT        NOT NULL,
    site_id         TEXT        NOT NULL,
    reading_ts      TIMESTAMPTZ NOT NULL,

    voltage_a       NUMERIC(8,3),
    voltage_b       NUMERIC(8,3),
    voltage_c       NUMERIC(8,3),
    current_a       NUMERIC(8,3),
    current_b       NUMERIC(8,3),
    current_c       NUMERIC(8,3),
    power_factor    NUMERIC(5,4),
    frequency_hz    NUMERIC(6,3),

    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_key      TEXT,

    CONSTRAINT meter_readings_pkey PRIMARY KEY (device_id, reading_ts)

) PARTITION BY RANGE (reading_ts);


-- Auto-partition procedure
-- Creates monthly partitions for a window of:
--   p_months_back  months in the past  (backfill / catch-up)
--   p_months_ahead months in the future (pre-create so inserts never fail)
--
-- Safe to call repeatedly – IF NOT EXISTS means already-existing partitions
-- are silently skipped, so the pg_cron job can run daily without errors.

CREATE OR REPLACE PROCEDURE create_monthly_partitions(
    p_months_back  INT DEFAULT 2,
    p_months_ahead INT DEFAULT 3
)
LANGUAGE plpgsql AS $$
DECLARE
    v_start        DATE;
    v_end          DATE;
    v_partition    TEXT;
    v_month        DATE;
BEGIN
    -- Walk from (today - p_months_back) to (today + p_months_ahead) inclusive
    FOR v_month IN
        SELECT generate_series(
            date_trunc('month', now())::date - (p_months_back  || ' months')::interval,
            date_trunc('month', now())::date + (p_months_ahead || ' months')::interval,
            '1 month'::interval
        )::date
    LOOP
        v_start     := date_trunc('month', v_month);
        v_end       := v_start + INTERVAL '1 month';
        v_partition := 'meter_readings_' || to_char(v_month, 'YYYY_MM');

        -- Dynamic DDL: only creates the partition if it does not already exist
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I
             PARTITION OF meter_readings
             FOR VALUES FROM (%L) TO (%L)',
            v_partition, v_start, v_end
        );

        RAISE NOTICE 'Partition % ready (% to %)', v_partition, v_start, v_end;
    END LOOP;
END;
$$;


-- Bootstrap: run the procedure immediately on first deploy.
-- Creates partitions from 2 months ago through 3 months ahead so the
-- table is ready to accept data straight away.
CALL create_monthly_partitions(
    p_months_back  => 2,
    p_months_ahead => 3
);


-- Schedule: run daily at 00:05 UTC via pg_cron.
-- Pre-creates the next 3 months of partitions so a partition always exists
-- before data for that month starts arriving.
-- The job is idempotent – re-running this script won't create duplicate jobs
-- because we delete any existing job with the same name first.
SELECT cron.unschedule(jobid)
FROM   cron.job
WHERE  jobname = 'smarterise-batch-monthly-partitions';

SELECT cron.schedule(
    'smarterise-batch-monthly-partitions',
    '5 0 1 * *',   -- 00:05 UTC on the 1st of every month
    $$CALL create_monthly_partitions(p_months_back => 2, p_months_ahead => 3)$$
);


CREATE INDEX IF NOT EXISTS idx_readings_site_ts
    ON meter_readings (site_id, reading_ts DESC);

CREATE INDEX IF NOT EXISTS idx_readings_device_ts
    ON meter_readings (device_id, reading_ts DESC);

CREATE INDEX IF NOT EXISTS idx_readings_ingested_recent
    ON meter_readings (ingested_at DESC)
    WHERE ingested_at > now() - INTERVAL '24 hours';


-- Materialised view: hourly_site_aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_site_aggregates AS
SELECT
    site_id,
    date_trunc('hour', reading_ts)                  AS hour_ts,
    COUNT(*)                                        AS reading_count,
    AVG(voltage_a)                                  AS avg_voltage_a,
    AVG(voltage_b)                                  AS avg_voltage_b,
    AVG(voltage_c)                                  AS avg_voltage_c,
    AVG((current_a + current_b + current_c) / 3.0) AS avg_current,
    AVG(power_factor)                               AS avg_power_factor,
    AVG(frequency_hz)                               AS avg_frequency_hz,
    MIN(reading_ts)                                 AS first_reading,
    MAX(reading_ts)                                 AS last_reading
FROM meter_readings
GROUP BY site_id, date_trunc('hour', reading_ts)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_hourly_agg_site_hour
    ON hourly_site_aggregates (site_id, hour_ts);


-- View: latest_readings_per_device
CREATE OR REPLACE VIEW latest_readings_per_device AS
SELECT DISTINCT ON (device_id)
    device_id,
    site_id,
    reading_ts,
    voltage_a, voltage_b, voltage_c,
    current_a, current_b, current_c,
    power_factor,
    frequency_hz,
    ingested_at
FROM meter_readings
ORDER BY device_id, reading_ts DESC;


-- Partition maintenance: drop partitions older than 24 months
-- Runs on the 1st of every month at 01:00 UTC.
-- Adjust p_retention_months to match your data retention policy.

CREATE OR REPLACE PROCEDURE drop_old_partitions(
    p_retention_months INT DEFAULT 24
)
LANGUAGE plpgsql AS $$
DECLARE
    v_cutoff    DATE;
    v_partition TEXT;
    v_rec       RECORD;
BEGIN
    v_cutoff := date_trunc('month', now()) - (p_retention_months || ' months')::interval;

    FOR v_rec IN
        SELECT child.relname AS partition_name
        FROM   pg_inherits
        JOIN   pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN   pg_class child  ON pg_inherits.inhrelid  = child.oid
        WHERE  parent.relname = 'meter_readings'
        AND    child.relname  < 'meter_readings_' || to_char(v_cutoff, 'YYYY_MM')
    LOOP
        RAISE NOTICE 'Dropping old partition: %', v_rec.partition_name;
        EXECUTE format('DROP TABLE IF EXISTS %I', v_rec.partition_name);
    END LOOP;
END;
$$;

SELECT cron.unschedule(jobid)
FROM   cron.job
WHERE  jobname = 'smarterise-drop-old-partitions';

SELECT cron.schedule(
    'smarterise-drop-old-partitions',
    '0 1 1 * *',   -- 01:00 UTC on the 1st of every month
    $$CALL drop_old_partitions(p_retention_months => 24)$$
);


COMMENT ON TABLE  meter_readings               IS 'Raw per-reading measurements from smart meters';
COMMENT ON TABLE  site_meter_map               IS 'Maps device_id to site; replaces app-code mapping';
COMMENT ON COLUMN meter_readings.reading_ts    IS 'Always UTC, normalised by Lambda before insert';
COMMENT ON PROCEDURE create_monthly_partitions IS 'Creates monthly range partitions; safe to call repeatedly; scheduled daily by pg_cron';
COMMENT ON PROCEDURE drop_old_partitions       IS 'Drops partitions older than p_retention_months; scheduled monthly by pg_cron';