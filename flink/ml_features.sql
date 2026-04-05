-- =============================================================
-- Flink Job: ML Feature Generation
-- Source:    enriched-events
-- Sink:      ml-features
-- Purpose:   Generate continuously updated rolling features per
--            facility using sliding windows. Features are designed
--            to feed downstream ML models for real-time inference
--            such as delivery time prediction or anomaly detection.
-- Pattern:   Sliding windows (HOP) produce overlapping feature
--            snapshots every 5 minutes over a 15-minute lookback.
--            This ensures models always consume fresh, stable
--            signals rather than stale batch features.
-- =============================================================

-- Sink table: rolling ML features per facility
CREATE TABLE `ml-features` (
    `key`                    VARCHAR(2147483647) NOT NULL,
    `facility_id`            VARCHAR(2147483647) NOT NULL,
    `facility_type`          VARCHAR(2147483647) NOT NULL,
    `region`                 VARCHAR(2147483647) NOT NULL,
    `window_start`           TIMESTAMP_LTZ(3) NOT NULL,
    `window_end`             TIMESTAMP_LTZ(3) NOT NULL,
    `rolling_order_volume`   BIGINT NOT NULL,
    `rolling_cancel_rate`    DOUBLE NOT NULL,
    `rolling_avg_value`      DOUBLE NOT NULL,
    `sla_breach_flag`        BOOLEAN NOT NULL
) DISTRIBUTED BY HASH(`key`) INTO 3 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'connector'      = 'confluent',
    'key.format'     = 'raw',
    'value.format'   = 'avro-registry'
);

-- ML feature generation job
-- HOP TVF syntax: slide every 5 min, window size 15 min
-- Produces one feature row per facility per slide interval
--
-- Features:
--   rolling_order_volume:  total events in window (throughput signal)
--   rolling_cancel_rate:   cancellations / total (quality signal)
--   rolling_avg_value:     avg order value (revenue signal)
--   sla_breach_flag:       true if any PROCESSING order stalled
--                          from FACILITY_ASSIGNED (operational signal)
INSERT INTO `ml-features`
SELECT
    facility_id                                             AS `key`,
    facility_id,
    facility_type,
    region,
    window_start,
    window_end,
    COUNT(*)                                                AS rolling_order_volume,
    CAST(
        COUNT(*) FILTER (WHERE order_state = 'CANCELLED')
        AS DOUBLE
    ) / CAST(COUNT(*) AS DOUBLE)                           AS rolling_cancel_rate,
    AVG(order_value)                                        AS rolling_avg_value,
    COUNT(*) FILTER (WHERE order_state = 'PROCESSING'
        AND previous_state = 'FACILITY_ASSIGNED') > 0      AS sla_breach_flag
FROM TABLE(
    HOP(
        TABLE `enriched-events`,
        DESCRIPTOR(`$rowtime`),
        INTERVAL '5' MINUTES,
        INTERVAL '15' MINUTES
    )
)
GROUP BY
    facility_id,
    facility_type,
    region,
    window_start,
    window_end;