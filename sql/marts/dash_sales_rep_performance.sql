/* =============================================================================
 * dash_sales_rep_performance
 * -----------------------------------------------------------------------------
 * Pre-aggregated mart at (month, sub_region, sales_rep, record_source)
 * grain, exposing actual amount, quota amount and attainment percent.
 *
 * Contains NO business logic beyond aggregation:
 *   - Rep allocation, currency conversion, and segment classification all
 *     happen upstream.
 * ============================================================================= */

WITH
  raw AS (
    SELECT
      DATE_TRUNC(event_date, MONTH)                AS plan_month,
      sub_region,
      sales_rep,
      record_source,
      SUM(COALESCE(total_amount_reporting, 0))     AS amount_reporting,
      SUM(COALESCE(quota_amount_reporting, 0))     AS quota_reporting
    FROM warehouse_curated.vw_sales_rep_allocation
    GROUP BY plan_month, sub_region, sales_rep, record_source
  ),

  /* Pivot actuals vs quota onto the same row per (month, sub_region, rep). */
  collapsed AS (
    SELECT
      plan_month,
      sub_region,
      sales_rep,
      SUM(CASE WHEN record_source = 'QUOTA' THEN 0
               ELSE amount_reporting END)          AS actual_amount_reporting,
      SUM(CASE WHEN record_source = 'QUOTA' THEN quota_reporting
               ELSE 0 END)                         AS quota_amount_reporting
    FROM raw
    GROUP BY plan_month, sub_region, sales_rep
  )

SELECT
  plan_month,
  sub_region,
  sales_rep,
  actual_amount_reporting,
  quota_amount_reporting,
  SAFE_DIVIDE(actual_amount_reporting, quota_amount_reporting) AS attainment_pct
FROM collapsed
;
