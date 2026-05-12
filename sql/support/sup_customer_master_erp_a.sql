/* =============================================================================
 * sup_customer_master_erp_a
 * -----------------------------------------------------------------------------
 * Customer master derived from ERP_A. Two countries are served by this
 * source, each with its own legal entity. Sales-rep assignment is derived
 * from a country + sub-region CASE-tree because ERP_A does not carry a
 * dedicated rep field.
 *
 * Pattern shown:
 *   - Normalize and trim source columns once in a CTE.
 *   - Join customers and resolve the in-scope segment (business_unit).
 *   - Deduplicate to one row per (record_source, customer_key).
 *   - Resolve sales_rep and sub_region using country/sub-region rules.
 *
 * Downstream consumer: stn_customer_master
 * ============================================================================= */

WITH
  invoices_normalized AS (
    SELECT
      origin_country                               AS country_origin,   -- 'A1' or 'A2'
      TRIM(customer_id)                            AS customer_key,
      TRIM(gl_class)                               AS gl_class,
      tax_id,
      UPPER(TRIM(country_code))                    AS bill_country,
      UPPER(TRIM(state_code))                      AS bill_state,
      transaction_date                             AS inclusion_date
    FROM warehouse_raw.erp_a_invoices
  ),

  base AS (
    SELECT
      i.country_origin,
      i.customer_key,
      i.gl_class,
      i.tax_id,
      i.bill_country,
      i.bill_state,
      UPPER(TRIM(c.customer_name))                 AS customer_name,
      TRIM(c.postal_code)                          AS postal_code,
      UPPER(TRIM(c.city))                          AS city,
      c.customer_type_l1,
      c.customer_type_l2,
      c.customer_type_l3,
      i.inclusion_date,

      /* Segment derivation by GL class — generic placeholders. */
      CASE
        WHEN i.gl_class IN ('CORE_A','CORE_B','CORE_C') THEN 'CORE_SEGMENT'
        WHEN i.gl_class IN ('LANG_A','LANG_B')          THEN 'LANGUAGE_SEGMENT'
        WHEN i.gl_class IN ('PROF_A','PROF_B')          THEN 'PROFESSIONAL_SEGMENT'
        ELSE 'OTHER_SEGMENT'
      END                                          AS business_unit
    FROM invoices_normalized i
    LEFT JOIN warehouse_raw.erp_a_customers c
      ON c.customer_id = i.customer_key
  ),

  in_scope AS (
    SELECT *
    FROM base
    WHERE business_unit = 'CORE_SEGMENT'
      AND TRIM(customer_key) <> ''
  ),

  deduped AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY country_origin, customer_key, business_unit
        ORDER BY inclusion_date DESC
      ) AS row_version
    FROM in_scope
  )

SELECT
  'ERP_A' AS record_source,

  CASE
    WHEN country_origin = 'A1' THEN 'Sales Org — Country A1'
    WHEN country_origin = 'A2' THEN 'Sales Org — Country A2'
  END AS sales_org_name,

  CASE
    WHEN country_origin = 'A1' THEN 'ENTITY_A1'
    WHEN country_origin = 'A2' THEN 'ENTITY_A2'
  END AS s1_entity,

  CASE
    WHEN country_origin = 'A1' THEN 'Country A1'
    WHEN country_origin = 'A2' THEN 'Country A2'
  END AS pl_country,

  customer_key                                     AS bill_to_customer_number,
  customer_key                                     AS ship_to_customer_number,

  /* Collapse synonyms and route private individuals to a canonical label. */
  CASE
    WHEN customer_name LIKE 'CONSUMER%'
      OR customer_name LIKE 'ECOMMERCE%'
      OR customer_name LIKE '%CASH SALE%'
      THEN 'PRIVATE INDIVIDUAL'
    ELSE customer_name
  END                                              AS bill_to_customer_name,

  bill_country                                     AS bill_to_location_country,
  bill_country                                     AS ship_to_location_country,
  postal_code                                      AS bill_to_location_postal_code,
  bill_state                                       AS bill_to_location_state,
  city                                             AS bill_to_location_city,

  tax_id,
  CAST(NULL AS STRING)                             AS tax_id_type,

  /* Channel classification (Direct / Distributor / Retail) using customer
     attributes and a fallback by name pattern. */
  CASE
    WHEN customer_name LIKE '%CONSUMER%'
      OR customer_type_l3 IN ('UNI','INST','GOV')
      OR customer_type_l1 IN ('SCHOOL','GOVT')
      THEN 'Direct'
    WHEN customer_type_l3 = 'DIS'
      OR customer_type_l1 = 'DISTRIBUTOR'
      OR customer_name LIKE '%DISTR%'
      THEN 'Distributor'
    WHEN customer_type_l3 = 'RET'
      OR customer_name LIKE '%BOOK%'
      THEN 'Retail'
    ELSE 'Unclassified'
  END                                              AS customer_segment,

  business_unit,

  CASE
    WHEN customer_type_l3 = 'INT'
      THEN 'Internal'
    ELSE 'External'
  END                                              AS intercompany_flag,

  /* Sales-rep assignment for ERP_A: country-driven; A2 has sub-region
     splits, A1 is one territory. */
  CASE
    WHEN country_origin = 'A1' THEN 'Rep — Country A1'

    WHEN country_origin = 'A2' AND bill_country IN ('XX','YY','ZZ')
      THEN 'Rep — Cluster Central'
    WHEN country_origin = 'A2' AND bill_country IN ('AA','BB','CC')
      THEN 'Rep — Cluster North'
    WHEN country_origin = 'A2' AND bill_country IN ('DD','EE','FF')
      THEN 'Rep — Cluster Caribbean'
    WHEN country_origin = 'A2' AND bill_country = 'A2'
      THEN 'Rep — Country A2'
    WHEN country_origin = 'A2' AND bill_country IN ('GG','HH','II')
      THEN 'Rep — Cluster Andean'
    ELSE 'Unassigned'
  END                                              AS sales_rep,

  CASE
    WHEN country_origin = 'A1' THEN 'Country A1'
    WHEN country_origin = 'A2' AND bill_country IN ('XX','YY','ZZ','AA','BB','CC','DD','EE','FF','GG','HH','II')
      THEN 'Multi-Country LATAM'
    WHEN country_origin = 'A2' THEN 'Country A2'
  END                                              AS sub_region,

  inclusion_date
FROM deduped
WHERE row_version = 1
;
