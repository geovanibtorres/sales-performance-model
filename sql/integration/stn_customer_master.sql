/* =============================================================================
 * stn_customer_master
 * -----------------------------------------------------------------------------
 * Conformed customer master across all regional ERPs in the cluster.
 *
 * Adding a new ERP is a two-step task:
 *   1. Create sup_customer_master_<source>.sql emitting the canonical
 *      columns expected here.
 *   2. Add a UNION ALL clause below.
 *
 * Downstream consumer: vw_sales_rep_allocation
 * ============================================================================= */

SELECT * FROM warehouse_curated.sup_customer_master_erp_a
UNION ALL
SELECT * FROM warehouse_curated.sup_customer_master_erp_b
UNION ALL
SELECT * FROM warehouse_curated.sup_customer_master_erp_c
;
