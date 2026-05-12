/* =============================================================================
 * sup_customer_master_erp_b
 * -----------------------------------------------------------------------------
 * Customer master derived from ERP_B (single-country ledger). The source
 * carries a dedicated rep field on each customer, which removes the need
 * for geography-based heuristics.
 *
 * Rep assignment source (erp_b_rep_contracts):
 *   This table is NOT a native ERP_B object. It is an Excel file owned by
 *   the Customer Services team and loaded weekly into the ERP_B raw
 *   schema via an incremental ingestion job. Each weekly delta is appended
 *   with a report_date, which is why we keep only the latest row per
 *   customer using ROW_NUMBER. Treating it as just-another-raw-table keeps
 *   the consumer code identical to a fully ERP-native flow.
 *
 * Pattern shown:
 *   - Pull the most recent rep assignment from the rep-contract table
 *     using ROW_NUMBER (one row per customer key).
 *   - Tag tax-id type by length to distinguish individuals from companies.
 *   - Filter to the in-scope segment.
 *
 * Downstream consumer: stn_customer_master
 * ============================================================================= */

WITH
  /* Latest rep assignment per customer, sourced from the rep-contract table. */
  rep_assignment AS (
    SELECT
      customer_id_norm,
      account_owner,
      account_group,
      report_date,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id_norm
        ORDER BY report_date DESC
      ) AS row_version
    FROM warehouse_raw.erp_b_rep_contracts
  ),
  rep_assignment_latest AS (
    SELECT * FROM rep_assignment WHERE row_version = 1
  ),

  invoices_normalized AS (
    SELECT
      TRIM(customer_id)                            AS customer_key,
      transaction_date                             AS inclusion_date,
      profit_center,
      accounting_classification
    FROM warehouse_raw.erp_b_invoices
    WHERE transaction_date >= DATE '2019-01-01'
  ),

  base AS (
    SELECT
      i.customer_key,
      i.inclusion_date,
      i.profit_center,
      i.accounting_classification,
      c.customer_name,
      c.country,
      c.state,
      c.city,
      c.postal_code,
      c.individual_tax_id,
      c.company_tax_id,
      r.account_owner,
      r.account_group,
      'CORE_SEGMENT' AS business_unit
    FROM invoices_normalized i
    LEFT JOIN warehouse_raw.erp_b_customers c
      ON c.customer_id = i.customer_key
    LEFT JOIN rep_assignment_latest r
      ON r.customer_id_norm = i.customer_key
    WHERE EXISTS (
      SELECT 1
      FROM warehouse_raw.erp_b_profit_centers p
      WHERE p.profit_center = i.profit_center
        AND p.segment       = 'CORE_SEGMENT'
    )
  ),

  deduped AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY customer_key, business_unit
        ORDER BY inclusion_date DESC
      ) AS row_version
    FROM base
  )

SELECT
  'ERP_B'                                          AS record_source,
  'Sales Org — Country B'                          AS sales_org_name,
  'ENTITY_B'                                       AS s1_entity,
  'Country B'                                      AS pl_country,

  customer_key                                     AS bill_to_customer_number,
  customer_key                                     AS ship_to_customer_number,

  COALESCE(account_group, customer_name)           AS bill_to_customer_name,

  COALESCE(country, 'Country B')                   AS bill_to_location_country,
  postal_code                                      AS bill_to_location_postal_code,
  state                                            AS bill_to_location_state,
  city                                             AS bill_to_location_city,

  CASE
    WHEN LENGTH(TRIM(individual_tax_id)) > 0 THEN individual_tax_id
    ELSE company_tax_id
  END                                              AS tax_id,
  CASE
    WHEN LENGTH(TRIM(individual_tax_id)) > 0 THEN 'individual'
    ELSE 'company'
  END                                              AS tax_id_type,

  /* Channel classification using profit center and tax-id type. */
  CASE
    WHEN customer_key IN ('DIST_001','DIST_002','DIST_003') THEN 'Distributor'
    WHEN profit_center IN ('PC_DIRECT_1','PC_DIRECT_2')
      OR LENGTH(TRIM(individual_tax_id)) > 0
      THEN 'Direct'
    ELSE 'Unclassified'
  END                                              AS customer_segment,

  business_unit,

  CASE
    WHEN accounting_classification = 'INTERCOMPANY' THEN 'Internal'
    ELSE 'External'
  END                                              AS intercompany_flag,

  COALESCE(account_owner, 'Unassigned')            AS sales_rep,

  /* Sub-region within Country B, derived from rep ownership. */
  CASE
    WHEN account_owner IN ('Rep — South B','Rep — South B Junior') THEN 'Country B — South'
    WHEN account_owner IN ('Rep — North B','Rep — North B Junior') THEN 'Country B — North'
    WHEN account_owner IN ('Rep — Government B')                   THEN 'Country B — Government'
    WHEN account_owner IN ('Rep — Key Accounts B')                 THEN 'Country B — Key Accounts'
    ELSE 'Country B — Unassigned'
  END                                              AS sub_region,

  inclusion_date
FROM deduped
WHERE row_version = 1
;
