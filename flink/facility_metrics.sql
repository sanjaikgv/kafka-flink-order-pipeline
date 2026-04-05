-- =============================================================
-- Flink Job: Windowed Facility Metrics + SLA Breach Detection
-- Source:    enriched-events
-- Sink:      facility-metrics
-- Purpose:   Compute per-facility operational metrics in 5-minute
--            tumbling windows. Detects SLA breaches, cancellation
--            rates, and high-value order throughput in real time.
-- Pattern:   Tumbling windows on $rowtime (Kafka arrival time).
--            In production, window on business event_timestamp
--            with explicit watermark to handle late-arriving events
--            from mobile or edge sources correctly.
-- =============================================================

-- Sink table: per-facility windowed metrics
-- 3 buckets matches facility-metrics topic partition count
-- key = facility_id for consistent downstream partitioning
CREATE TABLE `facility-metrics` (
    `key`                VARCHAR(2147483647) NOT NULL,
    `facility_id`        VARCHAR(2147483647) NOT NULL,
    `facility_type`      VARCHAR(2147483647) NOT NULL,
    `region`             VARCHAR(2147483647) NOT NULL,
    `window_start`       TIMESTAMP_LTZ(3) NOT NULL,
    `window_end`         TIMESTAMP_LTZ(3) NOT NULL,
    `total_orders`       BIGINT NOT NULL,
    `avg_order_value`    DOUBLE NOT NULL,
    `cancellation_count` BIGINT NOT NULL,
    `high_value_count`   BIGINT NOT NULL,
    `sla_breach_count`   BIGINT NOT NULL
) DISTRIBUTED BY HASH(`key`) INTO 3 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'connector'      = 'confluent',
    'key.format'     = 'raw',
    'value.format'   = 'avro-registry'
);

-- Windowed aggregation job
-- TUMBLE TVF syntax required by Confluent Cloud Flink
-- Groups events into fixed 5-minute non-overlapping windows
-- per facility, emitting one metrics row per window close.
--
-- Metrics computed per window per facility:
--   total_orders:       all events in window (volume signal)
--   avg_order_value:    average $ value (revenue signal)
--   cancellation_count: CANCELLED state events (quality signal)
--   high_value_count:   orders flagged >$200 (priority signal)
--   sla_breach_count:   PROCESSING orders from FACILITY_ASSIGNED
--                       used as a proxy for stalled orders
INSERT INTO `facility-metrics`
SELECT
    facility_id                                                AS `key`,
    facility_id,
    facility_type,
    region,
    window_start,
    window_end,
    COUNT(*)                                                   AS total_orders,
    AVG(order_value)                                           AS avg_order_value,
    COUNT(*) FILTER (WHERE order_state = 'CANCELLED')          AS cancellation_count,
    COUNT(*) FILTER (WHERE is_high_value = TRUE)               AS high_value_count,
    COUNT(*) FILTER (WHERE order_state = 'PROCESSING'
                     AND previous_state = 'PROCESSING') AS sla_breach_count
FROM TABLE(
    TUMBLE(
        TABLE `enriched-events`,
        DESCRIPTOR(`$rowtime`),
        INTERVAL '5' MINUTES
    )
)
GROUP BY
    facility_id,
    facility_type,
    region,
    window_start,
    window_end;