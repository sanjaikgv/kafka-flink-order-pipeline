-- =============================================================
-- Flink Job: Stream Enrichment
-- Source:    order-events
-- Sink:      enriched-events
-- Purpose:   Validate, clean, and enrich raw order events with
--            derived fields before downstream aggregation jobs.
-- Pattern:   Single-responsibility enrichment layer separates
--            data quality concerns from business logic.
-- =============================================================

-- Sink table: enriched events with derived fields
-- Distributed into 6 buckets to match source topic parallelism
CREATE TABLE `enriched-events` (
    `key`             VARCHAR(2147483647) NOT NULL,
    `order_id`        VARCHAR(2147483647) NOT NULL,
    `facility_id`     VARCHAR(2147483647) NOT NULL,
    `facility_type`   VARCHAR(2147483647) NOT NULL,
    `region`          VARCHAR(2147483647) NOT NULL,
    `order_state`     VARCHAR(2147483647) NOT NULL,
    `customer_id`     VARCHAR(2147483647) NOT NULL,
    `item_count`      INT NOT NULL,
    `order_value`     FLOAT NOT NULL,
    `event_timestamp` BIGINT NOT NULL,
    `previous_state`  VARCHAR(2147483647),
    `is_high_value`   BOOLEAN NOT NULL,
    `is_terminal`     BOOLEAN NOT NULL,
    `processing_time` TIMESTAMP_LTZ(3) NOT NULL
) DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'connector'      = 'confluent',
    'key.format'     = 'raw',
    'value.format'   = 'avro-registry'
);

-- Enrichment job
-- Derives three fields:
--   is_high_value: orders >$200, used for priority routing downstream
--   is_terminal:   flags DELIVERED/CANCELLED as final states, signals
--                  downstream jobs to close order-level state
--   processing_time: wall clock time Flink processed this event,
--                    used for pipeline latency monitoring
-- Data quality gate: nulls on critical fields are filtered here
-- so corrupt events never propagate to aggregation jobs
INSERT INTO `enriched-events`
SELECT
    facility_id                                       AS `key`,
    order_id,
    facility_id,
    facility_type,
    region,
    order_state,
    customer_id,
    item_count,
    order_value,
    event_timestamp,
    previous_state,
    order_value > 200.0                               AS is_high_value,
    order_state IN ('DELIVERED', 'CANCELLED')         AS is_terminal,
    CURRENT_TIMESTAMP                                 AS processing_time
FROM `order-events`
WHERE order_id IS NOT NULL
  AND facility_id IS NOT NULL
  AND order_state IS NOT NULL;