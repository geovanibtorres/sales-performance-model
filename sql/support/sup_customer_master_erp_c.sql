/* =============================================================================
 * sup_customer_master_erp_c
 * -----------------------------------------------------------------------------
 * Customer master derived from ERP_C (multi-country southern ledger).
 *
 * Pattern shown:
 *   - The ERP itself does not carry a sales-rep field at all.
 *   - The entire territory is owned by a single rep, applied as a constant.
 *   - This is the simplest end of the spectrum and is included to show that
 *     the contract is the same regardless of source complexity.
 *
 * Downstream consumer: stn_customer_master
 * ============================================================================= */

SELECT
  'ERP_C'                                          AS record_source,
  sales_org_name,
  s1_entity,
  pl_country,

  bill_to_customer_number,
  ship_to_customer_number,
  bill_to_customer_name,

  bill_to_location_country,
  bill_to_location_postal_code,
  bill_to_location_state,
  bill_to_location_city,

  tax_id,
  tax_id_type,

  customer_segment,
  business_unit,
  intercompany_flag,

  /* Single-rep territory — applied as a constant. */
  'Rep — Southern Cluster'                         AS sales_rep,
  'Southern Cluster'                               AS sub_region,

  inclusion_date
FROM warehouse_raw.erp_c_customer_master
WHERE business_unit = 'CORE_SEGMENT'
;
