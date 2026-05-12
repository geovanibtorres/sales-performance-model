/* =============================================================================
 * vw_sales_rep_allocation
 * -----------------------------------------------------------------------------
 * Sales-rep performance and quota view for the regional cluster.
 *
 * Two record types are unioned at compatible grain:
 *   1. Actuals — billings rows enriched with the resolved sales rep from
 *                stn_customer_master.
 *   2. Quota   — one row per (month, sales_rep, sub_region) from
 *                sup_sales_quota.
 *
 * Both halves share the same column list. Non-applicable columns are
 * emitted as NULL so a single dashboard can render them side-by-side
 * without joins.
 *
 * Filters applied to actuals:
 *   - Region scope: only cluster-owned ERPs, plus the subset of the global
 *     ERP rows owned commercially by the regional team.
 *   - Period scope: in-cycle fiscal year.
 *   - Segment scope: the business unit covered by the SIP.
 * ============================================================================= */

WITH
  cycle AS (
    SELECT
      DATE '2026-01-01' AS cycle_start,
      DATE '2026-12-31' AS cycle_end
  ),

  /* Actuals: regional ERPs (A, B, C) + carve-out of global ERP rows that
     belong to the regional team commercially. */
  actuals AS (
    SELECT
      b.record_source,
      b.transaction_date                           AS event_date,
      b.transaction_type,
      b.invoice_number,
      b.product_key,
      b.product_title,

      b.bill_to_customer_number,
      b.bill_to_customer_name,
      b.bill_to_location_country,
      b.bill_to_location_state,
      b.bill_to_location_city,
      b.functional_currency,

      b.quantity_net                               AS total_quantity,
      b.amount_net_local                           AS total_amount_local,
      b.amount_net_reporting                       AS total_amount_reporting,

      b.business_unit,
      'Cluster LATAM'                              AS pl_region_l1,
      cm.sub_region                                AS sub_region,
      cm.sales_rep                                 AS sales_rep,

      CAST(NULL AS NUMERIC)                        AS quota_amount_reporting
    FROM warehouse_curated.stn_billings b
    LEFT JOIN warehouse_curated.stn_customer_master cm
      ON  cm.record_source           = b.record_source
      AND cm.bill_to_customer_number = b.bill_to_customer_number
    CROSS JOIN cycle
    WHERE b.business_unit = 'CORE_SEGMENT'
      AND b.transaction_date BETWEEN cycle.cycle_start AND cycle.cycle_end
      AND (
            b.record_source IN ('ERP_A', 'ERP_B', 'ERP_C')
         OR (
              b.record_source = 'ERP_GLOBAL'
              AND cm.sales_rep IN ('Rep — Cluster Carve-out', 'Unassigned Cluster')
            )
      )
  ),

  /* Quota records: one row per (month, sales_rep, sub_region). */
  quota AS (
    SELECT
      'QUOTA'                                      AS record_source,
      q.plan_month                                 AS event_date,
      'QUOTA'                                      AS transaction_type,
      CAST(NULL AS STRING)                         AS invoice_number,
      CAST(NULL AS STRING)                         AS product_key,
      CAST(NULL AS STRING)                         AS product_title,

      CAST(NULL AS STRING)                         AS bill_to_customer_number,
      CAST(NULL AS STRING)                         AS bill_to_customer_name,
      CAST(NULL AS STRING)                         AS bill_to_location_country,
      CAST(NULL AS STRING)                         AS bill_to_location_state,
      CAST(NULL AS STRING)                         AS bill_to_location_city,
      'REPORTING'                                  AS functional_currency,

      CAST(NULL AS NUMERIC)                        AS total_quantity,
      CAST(NULL AS NUMERIC)                        AS total_amount_local,
      CAST(NULL AS NUMERIC)                        AS total_amount_reporting,

      'CORE_SEGMENT'                               AS business_unit,
      'Cluster LATAM'                              AS pl_region_l1,
      q.sub_region                                 AS sub_region,
      q.sales_rep                                  AS sales_rep,

      q.quota_amount_reporting                     AS quota_amount_reporting
    FROM warehouse_curated.sup_sales_quota q
    CROSS JOIN cycle
    WHERE q.plan_month BETWEEN cycle.cycle_start AND cycle.cycle_end
  )

SELECT * FROM actuals
UNION ALL
SELECT * FROM quota
;
