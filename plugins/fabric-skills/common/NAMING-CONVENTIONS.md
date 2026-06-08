# Naming Conventions (Canonical Authority)

This document is the **single source of truth** for naming Microsoft Fabric items in EDW
(Enterprise Data Warehouse) medallion deployments. `MEDALLION-CORE.md`,
`EDW-SOURCE-INGESTION.md`, and every `e2e-medallion-*` skill reference these rules. When a
skill needs a name, it follows the patterns here rather than inventing its own.

> **Scope**: batch, business-system EDW sources (ERP/CRM/HRM) on a Bronze/Silver/Gold
> medallion. Real-Time Intelligence (streaming/Eventhouse) naming is out of scope — use the
> existing RTI skills as-is.

---

## Core Principles

1. **The medallion layer appears in workspace and lakehouse *display names* only — never in schema names.** Schemas carry the **source system** (Bronze/Silver) or the **business domain** (Gold). Putting a layer index in a schema (e.g. `100_bronze`) would force backtick quoting in every Spark SQL reference — avoid it.
2. **Leading digits are allowed in display names** (workspaces, lakehouses) because they sort the layers visually, but **never in schema, table, view, or column names** that appear in SQL.
3. **Tables and views are snake_case with no layer prefix** — the schema already encodes layer/source/domain context.
4. **One canonical environment token**: `dev`, `tst`, `prd`. Use these everywhere; do not mix in `prod`, `qa`, `test`.
5. **Stable, decodable names beat clever names** — a name should tell you the layer, source/domain, and environment at a glance.

---

## Environment Tokens

| Environment | Token |
|-------------|-------|
| Development | `dev` |
| Test / QA   | `tst` |
| Production  | `prd` |

---

## Quick Reference

| Component | Convention | Example |
|-----------|------------|---------|
| Capacity | `cap_{project}_{env}` | `cap_acme_prd` |
| Workspace (shared) | `{index}_{Layer}_{env}` | `100_Bronze_prd` |
| Workspace (domain-separated) | `{DOMAIN}_{index}_{Layer}_{env}` | `FIN_100_Bronze_prd` |
| Lakehouse (**default**, per layer) | `LH_{index}_{Layer}` | `LH_100_Bronze` |
| Lakehouse (alternative, per source) | `LH_{index}_{Layer}_{source}` | `LH_100_Bronze_BC` |
| Warehouse | `WH_{index}_{Layer}_{domain}` | `WH_300_Gold_Sales` |
| Schema — Bronze/Silver | source system code | `bc`, `ln`, `jde` |
| Schema — Gold | business domain | `sales`, `finance`, `hr` |
| Table | snake_case, no layer prefix | `sales_order_lines` |
| Materialized Lake View | snake_case, no prefix | `daily_sales_summary` |
| Notebook | `NB_{Layer}_{purpose}` | `NB_Bronze_BC_Ingest` |
| Pipeline | `PL_{purpose}` | `PL_BC_Daily_Refresh` |
| Airflow DAG | `dag_{purpose}` | `dag_bc_daily_refresh` |
| Semantic model | `SM_{domain}` | `SM_Sales` |
| Report | `RPT_{audience}_{purpose}` | `RPT_Exec_Sales_KPI` |
| Variable library | `VL_{scope}` | `VL_BC_Connections` |

---

## Layer Index Scheme

A leading index orders layers in any alphabetical workspace/lakehouse list:

| Layer | Index | Purpose |
|-------|-------|---------|
| Bronze | `100` | Raw landing, as-received |
| Silver | `200` | Cleaned, conformed, deduplicated |
| Gold | `300` | Curated business marts for BI |

---

## Workspaces

### Default: three per-layer workspaces

The default topology is **one workspace per layer**, giving clean RBAC boundaries —
engineers own Bronze/Silver, analysts consume Gold:

```
100_Bronze_prd
200_Silver_prd
300_Gold_prd
```

### Domain / security-boundary separation

When a business domain must be **kept isolated** for compliance or data-sensitivity reasons
(common for **Finance, HR, and Operations**), do **not** co-mingle it in the shared per-layer
workspaces. Instead, stand up a **dedicated per-layer workspace set** for that domain, with a
short uppercase domain code prefix:

```
FIN_100_Bronze_prd      HR_100_Bronze_prd      OPS_100_Bronze_prd
FIN_200_Silver_prd      HR_200_Silver_prd      OPS_200_Silver_prd
FIN_300_Gold_prd        HR_300_Gold_prd        OPS_300_Gold_prd
```

Guidance:

- Use domain separation **only where a real isolation requirement exists** (e.g. HR/payroll data, regulated finance data). Domains without a separation requirement stay in the shared `{index}_{Layer}_{env}` workspaces and are separated by **schema** instead.
- Keep domain codes short and stable: `FIN` (Finance), `HR` (Human Resources), `OPS` (Operations), `SAL` (Sales).
- The domain code is the **security boundary marker**. Within a non-separated workspace, the equivalent boundary is the Gold schema name (`finance`, `hr`, `operations`).

---

## Lakehouses

### Default: per layer, source as schema

The **default** strategy is **one lakehouse per layer**, with each source system landing in its
own **schema**. This keeps the lakehouse count flat as sources are added:

```
LH_100_Bronze     schemas: bc, ln, jde, ...
LH_200_Silver     schemas: bc, ln, jde, ...
LH_300_Gold       schemas: sales, finance, hr, ...
```

Source isolation in this model is enforced with **schema-level access control**. This is the
recommended pattern for most deployments.

### Alternative: per source, per layer

When a source system needs hard physical isolation (separate lifecycle, separate ownership, or a
strict blast-radius boundary), use **one lakehouse per source per layer**:

```
LH_100_Bronze_BC     LH_100_Bronze_LN
LH_200_Silver_BC     LH_200_Silver_LN
```

Trade-off: cleaner isolation, but the lakehouse count multiplies with every source
(N sources × 3 layers). Reserve this for the few sources that genuinely require it; do not make
it the blanket default.

> **Lakehouse `display name` vs SQL endpoint**: the `LH_100_Bronze` display name is what you see
> in the workspace. The SQL analytics endpoint exposes the **schemas and tables**, not the layer
> index — so SQL consumers reference `bc.sales_order_lines`, never `LH_100_Bronze.bc...`.

---

## Schemas

- **Bronze and Silver schemas = source system code.** Keep these short, lowercase, and stable: `bc` (Business Central), `ln` (Infor LN), `jde` (JD Edwards), `sap`, `sf` (Salesforce), `wd` (Workday).
- **Gold schemas = business domain.** `sales`, `finance`, `hr`, `operations`, `supply_chain`.
- Schema names are **lowercase snake_case, no leading digits, no layer prefix.**

---

## Tables, Views, and Materialized Lake Views

- **snake_case, no layer/source prefix** — the schema carries that context.
- Bronze tables retain source-native grain and add metadata columns (see `MEDALLION-CORE.md`).
- Silver tables are conformed/deduplicated business entities: `sales_order_lines`, `gl_transactions`, `employee_assignments`.
- Gold tables/MLVs are analytics-ready marts: `daily_sales_summary`, `monthly_gl_balance`, `headcount_by_dept`.
- **Materialized Lake Views follow the same table rules** — snake_case, no prefix. The "view" nature is a property of the object, not the name.

Fully qualified reference in Spark SQL:

```sql
-- {schema}.{table} within the layer's lakehouse
select *
  from bc.sales_order_lines
```

---

## Source System Codes (extend as needed)

| Source system | Code |
|---------------|------|
| Microsoft Dynamics 365 Business Central | `bc` |
| Infor LN | `ln` |
| Oracle JD Edwards | `jde` |
| SAP | `sap` |
| Salesforce | `sf` |
| Workday | `wd` |

---

## Anti-Patterns (do not do)

- ❌ Layer index or name inside a schema: `100_bronze.orders`, `bronze_bc.orders` — forces quoting, defeats the schema's purpose.
- ❌ Layer prefix on table names: `bronze_sales_order_lines`.
- ❌ Mixed environment tokens: `prod`, `qa`, `Test`.
- ❌ Hardcoded GUIDs in names or references — discover IDs via the Fabric REST API (see `COMMON-CORE.md`).
- ❌ Putting an isolated domain (HR/Finance) into a shared workspace and relying on hope instead of a workspace/schema boundary.
