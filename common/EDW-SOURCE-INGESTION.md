# EDW Source Ingestion (Business Systems → Bronze)

How batch business-system data (ERP / CRM / HRM) lands in the **Bronze** layer of a Fabric
medallion. Bronze is the only layer that touches external sources; Silver and Gold read from
OneLake. This document is **transformation-agnostic** — the same Bronze landing serves the MLV,
Warehouse T-SQL, and Notebook patterns.

> **Naming**: Bronze schemas are **source system codes** (`bc`, `ln`, `jde`, …) per
> [NAMING-CONVENTIONS.md](./NAMING-CONVENTIONS.md). Land each source into its own schema in
> `LH_100_Bronze`.

---

## Ingestion Methods

| Method | Best for | Lands as | Managed Delta table? |
|--------|----------|----------|----------------------|
| **Database / Open Mirroring** | Sources with a supported mirror (e.g. Business Central, SQL-based ERPs) — near-real-time, low-config replication | Mirrored Delta in OneLake | Mirrored tables are read-only replicas — **shortcut or copy into a managed table** before MLV use |
| **Fivetran (or similar SaaS ELT)** | Broad SaaS/ERP/CRM coverage (Salesforce, Workday, NetSuite) with managed connectors & schema drift handling | Delta tables in a Fabric destination | Verify the connector writes **managed** tables; if external, materialize |
| **Data Factory pipeline Copy activity** | Recurring scheduled pulls from databases, files, APIs, cloud storage | Files in `Files/` or Delta tables | Copy to `Tables/` (`saveAsTable`) for managed tables |
| **OneLake shortcuts** | Data already in ADLS Gen2, S3, GCS, or another OneLake location — avoid duplication | Virtual reference (no copy) | Shortcuts are **not managed tables** — materialize before MLV use |
| **OneLake API / `curl`** | One-off or scripted file uploads | Files in `Files/` | Read + `saveAsTable` to create managed table |

---

## Choosing a Method

1. **Is there a first-party mirror for the source?** (e.g. Business Central, mirrored databases) → prefer **Mirroring** for the lowest-maintenance near-real-time replica.
2. **Is it a SaaS app with a mature managed connector?** → **Fivetran** (or equivalent) handles auth, incremental sync, and schema drift.
3. **Is it a database/file/API on a schedule with no mirror?** → **Data Factory Copy activity**.
4. **Is the data already in OneLake / a cloud lake?** → **OneLake shortcut** (no copy), then materialize if a managed table is required downstream.

Prefer **incremental** loads (watermark / CDC) over full reloads wherever the source supports it.

---

## The Managed-Table Requirement (read before choosing)

Several methods produce **read-only replicas or virtual references**, not Lakehouse-managed
tables. This matters because the **MLV pattern requires managed source tables** (created via
`saveAsTable`), and **incremental MLV refresh additionally requires Change Data Feed on the
source Delta tables**. So:

- **Mirrored tables and shortcuts are not directly usable as MLV sources.** Stage them into a managed Bronze (or Silver staging) table first.
- Enable Change Data Feed on managed source tables intended for incremental MLV refresh:

  ```sql
  alter table bc.sales_order_lines
   set tblproperties (delta.enableChangeDataFeed = true)
  ```

- Materialize a shortcut/mirror into a managed table when needed:

  ```python
  (spark.read.format("delta").load("Files/shortcuts/bc/sales_order_lines")
        .write.format("delta").mode("overwrite")
        .saveAsTable("bc.sales_order_lines"))
  ```

See [`e2e-medallion-mlv`](../skills/e2e-medallion-mlv/SKILL.md) for the full MLV refresh constraints.

---

## Bronze Landing Standards

Regardless of method, Bronze tables should:

- **Preserve source grain and history** — append, do not overwrite away history; never clean or filter on the way in.
- **Add ingestion metadata columns**: `ingestion_timestamp`, `source_system`, `source_file` / batch ID.
- **Use the source-system schema**: `bc`, `ln`, `jde`, etc.
- **Be Delta managed tables** when they feed an MLV pipeline.

```python
from pyspark.sql.functions import current_timestamp, lit

(df.withColumn("ingestion_timestamp", current_timestamp())
   .withColumn("source_system", lit("bc"))
   .write.format("delta").mode("append")
   .saveAsTable("bc.sales_order_lines"))
```

---

## Source-System Notes (ERP / CRM / HRM)

- **ERP** (Business Central, Infor LN, JD Edwards, SAP): high table counts, normalized schemas, frequent deletes/updates. Note the **append-only** constraint for incremental MLV refresh — updates/deletes in the source force a full refresh even with CDF enabled. Plan refresh cadence accordingly.
- **CRM** (Salesforce, Dynamics): wide, sparsely-populated objects; mature managed connectors exist — Fivetran-style ELT is usually the lowest-effort path.
- **HRM** (Workday, SAP SuccessFactors): sensitive payroll/PII data — land into a **domain-separated** workspace set (`HR_100_Bronze_{env}`) per [NAMING-CONVENTIONS.md § Domain separation](./NAMING-CONVENTIONS.md#domain--security-boundary-separation), and apply schema/workspace-level RBAC from the start.

---

## Companion Documents

- [NAMING-CONVENTIONS.md](./NAMING-CONVENTIONS.md) — schema/table naming.
- [MEDALLION-CORE.md](./MEDALLION-CORE.md) — layer concepts, topology, RBAC.
- [COMMON-CLI.md](./COMMON-CLI.md) — OneLake data access, `az rest`, tokens.
