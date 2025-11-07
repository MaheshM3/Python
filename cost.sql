WITH jobs_usage AS (
  SELECT
    *,
    usage_metadata.job_id,
    usage_metadata.job_run_id AS run_id,
    usage_metadata.node_type,
    usage_metadata.product_features.is_photon AS is_photon,
    identity_metadata.run_as AS run_as
  FROM system.billing.usage
  WHERE billing_origin_product = 'JOBS'
    AND usage_type = 'COMPUTE_TIME'  -- Focus on compute costs
    AND usage_metadata.job_run_id = 'your-job-run-id'
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
    node_type,  -- Breakdown by driver/worker instance type
    is_photon,  -- Breakdown by Photon usage
    FIRST(run_as, TRUE) AS run_as,
    sku_name,
    SUM(usage_usd) AS usage_usd,
    SUM(usage_quantity) AS usage_quantity
  FROM jobs_usage_with_usd
  GROUP BY ALL  -- Groups by node_type, is_photon, etc.
),
timeline_agg AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    MIN(period_start_time) AS run_start_time,
    MAX(period_end_time) AS run_end_time,
    SUM(TIMESTAMPDIFF(SECOND, period_start_time, period_end_time)) AS execution_duration_seconds,
    FIRST(result_state, TRUE) OVER (PARTITION BY run_id ORDER BY period_end_time DESC) AS result_state  -- Only in final row for long runs
  FROM system.lakeflow.job_run_timeline
  GROUP BY workspace_id, job_id, run_id
)
SELECT
  t1.*,
  t3.run_start_time,
  t3.run_end_time,
  t3.execution_duration_seconds,
  t3.result_state,
  SUM(usage_usd) OVER (PARTITION BY t1.run_id) AS total_run_usd  -- Optional: Add total for context
FROM jobs_usage_aggregated t1
LEFT JOIN timeline_agg t3 USING (workspace_id, job_id, run_id)
ORDER BY t3.is_photon DESC, t1.node_type, usage_start_time;