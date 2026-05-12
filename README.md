# Sales Representative Allocation & Quota Attainment

A reference implementation of a sales-rep allocation and quota-attainment
analytical model for a multi-country regional cluster. The model unifies
billings from three regional ERPs with a quota plan delivered as a
spreadsheet, and exposes a single view aligned to the structure expected by
sales-incentive and pipeline dashboards.

This repository is a **generic, anonymized portfolio version** of a
production analytics codebase. All identifiers — table names, column names,
business units, geographies, sales-rep names, account codes, vendor systems,
currencies — have been replaced with neutral equivalents. The intent is to
showcase the **modeling discipline, source-isolation pattern, and rep
allocation reasoning**, not any proprietary content.

---

## 1. Business Problem (abstracted)

A regional cluster of countries (referred to here as the **LATAM region**)
sells the same product line through three different transactional systems,
each inherited from a different operating company:

- **ERP_A** — ledger for two countries in the cluster.
- **ERP_B** — ledger for the largest country in the cluster.
- **ERP_C** — ledger for the remaining countries in the cluster.

Each ERP carries its own customer master, sales-rep assignment rules,
country naming, and currency. On top of that, the regional sales leadership
sets **monthly sales quotas (SIP — Sales Incentive Plan)** in a spreadsheet
that is loaded into the warehouse via a managed prep workflow.

The analytical questions to answer are:

- Who is the **sales rep responsible** for each invoice line, regardless of
  which ERP it came from?
- How is each rep performing **against quota**, by month and by sub-region?
- How does **regional roll-up** look, including reps that sell across more
  than one source system?
- How can finance and commercial teams reconcile actuals vs target without
  reconciling between three different ERPs?

---

## 2. Architecture

The platform follows the same **layered, source-isolated** pattern used for
the upstream billings model. Each regional ERP gets its own customer-master
file; the integration layer unions them; the mart layer combines actuals
with quota.

```
 ┌────────────┐   ┌────────────┐   ┌──────────────┐
 │  sup_*     │ → │   stn_*    │ → │  vw_/dash_*  │
 │ (per src)  │   │ (union)    │   │  (consume)   │
 └────────────┘   └────────────┘   └──────────────┘
        ▲
        │
 ┌─────────────┐
 │  Excel +    │
 │  prep flow  │
 │  → sup_*    │
 └─────────────┘
```

### Layer responsibilities

| Layer    | Prefix   | Purpose                                                                  |
|----------|----------|--------------------------------------------------------------------------|
| Support  | `sup_`   | Per-source customer master (with rep assignment) + quota table.          |
| Integrate| `stn_`   | Union of all per-source customer masters into one conformed surface.     |
| Mart     | `vw_`/`dash_` | Final reporting surfaces: rep allocation view + performance aggregate. |

---

## 3. Data Preparation Layer

Each per-source `sup_customer_master_*` file resolves the **sales-rep
assignment** for that ERP. Three patterns appear, in order of preference:

1. **Direct mapping in source data.** A field on the customer or invoice
   (e.g. an `executive_code`) names the sales rep.
2. **Geography-based fallback.** When the source has no rep field, country
   and sub-region columns drive a CASE-tree.
3. **Customer-attribute heuristic.** Type of customer (institution vs
   distributor vs bookshop vs private individual) decides the rep when
   neither of the above is available.

In all three cases the file emits the **same canonical customer-master
contract**:

```
record_source             -- which ERP this row came from
sales_org_name            -- legal entity / sales org
pl_country                -- reporting country
bill_to_customer_number   -- standardized customer key
bill_to_customer_name     -- standardized name (synonyms collapsed)
ship_to_*                 -- mirror of bill_to_*
business_unit             -- conformed segment
intercompany_flag         -- internal vs external
sales_rep                 -- canonical rep handle (sanitized to roles here)
sub_region                -- reporting cluster within the cluster
```

Deduplication is done with `ROW_NUMBER OVER (PARTITION BY src, customer_key
ORDER BY inclusion_date DESC)` to keep only the most recent row per
customer per source.

### Quota Source (Excel → Warehouse)

The SIP quota dataset is owned by the sales-ops team and lives as an Excel
file. It is loaded into the warehouse via a managed **Tableau Prep**
workflow, which produces the table consumed here as
[sup_sales_quota](sql/support/sup_sales_quota.sql). The grain is one row
per `(month, sales_rep, sub_region)` with a single quota measure expressed
in the reporting currency.

This is a deliberate choice:

- The quota changes monthly and is owned by a non-engineering team.
- Editing in Excel is cheap; editing a database is not.
- The prep workflow validates schema and types before publishing, so the
  warehouse only sees clean rows.

### Rep-Contract Source for ERP_B (Excel → ERP_B raw)

ERP_B has a dedicated rep field on the customer record, but the
authoritative rep-to-customer mapping does **not** originate inside the
ERP. It comes from a separate **Excel file owned by the Customer Services
team** (`erp_b_rep_contracts`, the sanitized name for the original
`EXT_TB_CONTRATOS`), which is loaded into the ERP_B raw schema on a
**weekly incremental** schedule. Each weekly delta is appended with a
`report_date`, and [sup_customer_master_erp_b](sql/support/sup_customer_master_erp_b.sql)
keeps only the latest row per customer using `ROW_NUMBER`.

The rationale mirrors the quota source:

- Contract ownership shifts are managed operationally by Customer
  Services, not by IT — Excel is the natural editing surface.
- A weekly incremental load is enough resolution for sales-rep
  attribution, and it preserves history for audit.
- Once the file lands in the ERP_B raw schema, the downstream code treats
  it like any other raw table — no special path is needed in the
  integration or mart layers.

---

## 4. Analytical Model

The mart layer exposes two artefacts:

### 4.1 [vw_sales_rep_allocation](sql/marts/vw_sales_rep_allocation.sql)

A `UNION ALL` of two compatible record types:

- **Actuals** — billings rows tagged with the resolved sales rep.
- **Quota**  — one row per rep per month with the planned target.

A `record_source` column distinguishes the two record types so a single
dashboard can render them side-by-side without joins. The two halves share
identical column lists; non-applicable columns are emitted as `NULL`.

Filters applied to actuals:
- **Region scope** — only the LATAM cluster sources, plus the subset of the
  global ERP rows that the regional team owns commercially.
- **Period scope** — the in-cycle fiscal year.
- **Segment scope** — only the business unit covered by the SIP.

### 4.2 [dash_sales_rep_performance](sql/marts/dash_sales_rep_performance.sql)

Pre-aggregated mart at `(month, sub_region, sales_rep, record_source)`
grain. Exposes:

- `actual_amount_reporting` — net billings in reporting currency.
- `quota_amount_reporting`  — target.
- `attainment_pct`          — actual / quota, computed once.

The mart contains **no business logic** beyond aggregation; rep allocation,
currency conversion and segment classification all happen upstream.

---

## 5. Why the Solution Scales

- **Per-source isolation.** A schema change in one ERP is one file to
  update; the integration and mart layers are unaffected as long as the
  customer-master contract is preserved.
- **Pluggable quota source.** The quota is one table at a known grain. A
  future replacement (e.g. a planning platform instead of Excel) only needs
  to publish the same shape; no downstream change is required.
- **Single rep allocation rule per source.** All rep assignment logic for
  ERP_X lives in `sup_customer_master_erp_x.sql`. There is exactly one
  place to change when sub-region ownership shifts.
- **Single rep allocation rule per source.** All rep assignment logic for
  ERP_X lives in `sup_customer_master_erp_x.sql`. There is exactly one
  place to change when sub-region ownership shifts.
- **Stateless rebuilds.** The mart is a pure function of staging + quota.
  Backfills, mid-year quota restatements and territory reorganizations are
  trivial reprocessings.
- **Auditability.** Any actual on the dashboard can be traced back through
  `dash_ → vw_ → stn_ → sup_` to a single source row.

---

## Repository Layout

```
sales-rep-allocation-portfolio/
├── README.md
├── .gitignore
└── sql/
    ├── support/
    │   ├── sup_customer_master_erp_a.sql
    │   ├── sup_customer_master_erp_b.sql
    │   ├── sup_customer_master_erp_c.sql
    │   └── sup_sales_quota.sql
    ├── integration/
    │   └── stn_customer_master.sql
    └── marts/
        ├── vw_sales_rep_allocation.sql
        └── dash_sales_rep_performance.sql
```

---

## Notes

- All table, column, country and rep references are illustrative.
- The three ERPs are abstracted as `ERP_A`, `ERP_B`, `ERP_C`. They map
  conceptually to a regional ledger, a country-specific ledger, and a
  multi-country ledger respectively.
- Sub-region names (`Cluster N`, `Cluster S`, etc.) are placeholders.
- Sales-rep names are abstracted as roles (`Rep — <Sub-region>`).
