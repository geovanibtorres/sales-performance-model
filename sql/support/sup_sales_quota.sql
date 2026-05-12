/* =============================================================================
 * sup_sales_quota
 * -----------------------------------------------------------------------------
 * Sales quota (SIP — Sales Incentive Plan) per rep, per sub-region, per
 * month, expressed in the reporting currency.
 *
 * Source: spreadsheet maintained by the regional sales-ops team.
 * Loader: managed prep workflow that validates schema and types and
 *         publishes one row per (month, sub_region, sales_rep).
 *
 * Why a spreadsheet:
 *   - The plan changes frequently (monthly).
 *   - Owned by a non-engineering team.
 *   - Editing in Excel is cheap; database edits are not.
 *
 * Downstream consumer: vw_sales_rep_allocation
 * ============================================================================= */

SELECT
  CAST(plan_month AS DATE)                         AS plan_month,
  sales_rep                                        AS sales_rep,
  sub_region                                       AS sub_region,
  CAST(quota_amount_reporting AS NUMERIC)          AS quota_amount_reporting
FROM warehouse_curated.sales_quota_excel_loaded
WHERE quota_amount_reporting IS NOT NULL
;
