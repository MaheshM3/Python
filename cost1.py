WITH jobs_usage AS (
  SELECT
    *,
    usage_metadata.job_id,
    usage_metadata.job_run_id AS run_id,
    usage_metadata.node_type,
    usage_metadata.product_features.is_photon AS is_photon,
    custom_tags.config_test AS config_name,
    identity_metadata.run_as AS run_as
  FROM system.billing.usage
  WHERE billing_origin_product = 'JOBS'
    AND usage_type = 'COMPUTE_TIME'
    AND usage_metadata.job_run_id IN ('run_id_1', 'run_id_2', 'run_id_3')  -- Your test run IDs
    AND custom_tags.config_test IS NOT NULL  -- Filter tagged tests
    AND sku_name NOT LIKE '%SERVERLESS%'  -- Exclude serverless
    AND record_type = 'ORIGINAL'  -- Ignore retractions/restatements for net usage
    AND usage_quantity > 0  -- Double-check: Skip any negative adjustments
),
jobs_usage_with_usd AS (
  SELECT
    jobs_usage.*,
    usage_quantity * pricing.default AS usage_usd
  FROM jobs_usage
  LEFT JOIN system.billing.list_prices pricing ON
    jobs_usage.sku_name = pricing.sku_name
    AND pricing.price_start_time <= jobs_usage.usage_start_time
    AND (pricing.price_end_time >= jobs_usage.usage_start_time OR pricing.price_end_time IS NULL)
    AND pricing.currency_code = 'USD'
),
jobs_usage_aggregated AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    config_name,
    node_type,  -- Groups breakdowns (e.g., driver vs. worker types)
    is_photon,  -- Groups Photon vs. non-Photon slices
    FIRST(run_as, TRUE) AS run_as,
    sku_name,
    SUM(usage_usd) AS total_usd,    -- Sums across multiple interval records
    SUM(usage_quantity) AS total_dbus  -- Sums DBUs across multiples
  FROM jobs_usage_with_usd
  GROUP BY ALL  -- Aggregates by run_id + breakdown dims; handles 100s of rows per run
),
timeline_agg AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    custom_tags.config_name,
    MIN(period_start_time) AS run_start_time,
    MAX(period_end_time) AS run_end_time,
    SUM(TIMESTAMPDIFF(SECOND, period_start_time, period_end_time)) AS execution_duration_seconds,
    FIRST(result_state, TRUE) OVER (PARTITION BY run_id ORDER BY period_end_time DESC) AS result_state
  FROM system.lakeflow.job_run_timeline
  GROUP BY workspace_id, job_id, run_id, custom_tags.config_name
)
SELECT
  t1.config_name,
  t1.node_type,
  t1.is_photon,
  t1.total_usd,
  t1.total_dbus,
  t3.execution_duration_seconds / 60.0 AS duration_minutes,
  t1.total_usd / (t3.execution_duration_seconds / 3600.0) AS usd_per_hour,  -- Efficiency metric
  t3.result_state,
  COUNT(*) OVER (PARTITION BY t1.run_id) AS raw_record_count  -- Optional: Shows # of original records summed
FROM jobs_usage_aggregated t1
LEFT JOIN timeline_agg t3 USING (workspace_id, job_id, run_id, config_name)
ORDER BY t1.config_name, t3.execution_duration_seconds;