---
name: e2e-medallion-mlv
description: >
  Implement an end-to-end batch EDW Medallion (Bronze/Silver/Gold) in Microsoft Fabric using
  Materialized Lake Views (MLVs) — declarative Spark SQL CREATE MATERIALIZED LAKE VIEW objects
  with Fabric-managed optimal/incremental/full refresh and per-MLV schedules. Use when the user
  wants to: (1) build a medallion where layer-to-layer transformations are declarative SQL views
  rather than imperative notebooks, (2) author CREATE MATERIALIZED LAKE VIEW definitions for
  Silver/Gold, (3) set up incremental refresh with Change Data Feed for ERP/CRM/HRM sources,
  (4) schedule and orchestrate MLV refresh, (5) connect Gold MLVs to Power BI via Direct Lake.
  Triggers: "materialized lake view", "materialized lake views", "MLV", "CREATE MATERIALIZED LAKE VIEW",
  "declarative medallion", "SQL medallion", "incremental refresh medallion", "lakehouse views pipeline".
---

> **Update Check — ONCE PER SESSION (mandatory)**
> The first time this skill is used in a session, run the **check-updates** skill before proceeding.
> - **GitHub Copilot CLI / VS Code**: invoke the `check-updates` skill.
> - **Claude Code / Cowork / Cursor / Windsurf / Codex**: compare local vs remote package.json version.
> - Skip if the check was already performed earlier in this session.

> **CRITICAL NOTES**
> 1. To find workspace details (including its ID) from a workspace name: list all workspaces, then filter with JMESPath.
> 2. To find item details (including its ID) from workspace ID, item type, and item name: list all items of that type in that workspace, then filter with JMESPath.
> 3. **Recency**: MLV capabilities change quickly. Verify refresh behavior and supported SQL constructs against the current Microsoft Learn docs linked at the bottom before asserting limits.

# End-to-End Medallion Architecture — Materialized Lake Views

This is **pattern 1 of three** EDW medallion patterns (see
[MEDALLION-CORE.md](../../common/MEDALLION-CORE.md)). All three are the same Bronze/Silver/Gold
architecture; this one uses **declarative Spark SQL Materialized Lake Views** as the
transformation engine between layers, with Fabric managing refresh.

## Prerequisite Knowledge

Read these companion documents first — they hold the foundational context this skill depends on:

- [NAMING-CONVENTIONS.md](../../common/NAMING-CONVENTIONS.md) — **canonical** workspace/lakehouse/schema/table/MLV naming
- [MEDALLION-CORE.md](../../common/MEDALLION-CORE.md) — layer concepts, workspace/lakehouse topology, RBAC, Gold→Power BI
- [EDW-SOURCE-INGESTION.md](../../common/EDW-SOURCE-INGESTION.md) — landing ERP/CRM/HRM data into Bronze (and the managed-table requirement)
- [COMMON-CORE.md](../../common/COMMON-CORE.md) — Fabric REST API patterns, auth, item discovery
- [COMMON-CLI.md](../../common/COMMON-CLI.md) — `az rest`, token acquisition, SQL/TDS access
- [SPARK-AUTHORING-CORE.md](../../common/SPARK-AUTHORING-CORE.md) — lakehouse creation, notebook deployment, job execution

---

## What an MLV Is

A **Materialized Lake View** is a Delta table whose contents are defined by a Spark SQL query and
kept up to date by Fabric. You declare the *what* (a `SELECT`); Fabric decides the *how* of
refresh (skip / incremental / full) and tracks lineage across the chain of MLVs.

- **Engine**: declarative Spark SQL `CREATE MATERIALIZED LAKE VIEW ... AS SELECT ...`
- **Orchestration**: per-MLV schedules (multi-schedule supported at GA); managed MLVs in a Lakehouse get dependency-aware refresh, retries, and monitoring
- **Best fit**: medallion transforms that express cleanly as SQL — most Silver conformance and Gold aggregation. For imperative/multi-step logic, use the notebook pattern instead.

MLVs are **generally available** (introduced in preview at Build 2025; GA in 2026 with
multi-schedule, broader incremental refresh, PySpark authoring, in-place updates, and stronger
data-quality controls).

---

## Must/Prefer/Avoid

### MUST DO
- **Use managed Lakehouse tables as MLV sources** — created via `saveAsTable`, not external Delta paths (`.save(path)`). Mirrored replicas and OneLake shortcuts are **not** managed tables; materialize them first (see [EDW-SOURCE-INGESTION.md](../../common/EDW-SOURCE-INGESTION.md#the-managed-table-requirement-read-before-choosing)).
- **Enable Change Data Feed only on Bronze Delta tables you land yourself** (your own `saveAsTable` writes) to drive incremental refresh:
  ```sql
  alter table bc.sales_order_lines
   set tblproperties (delta.enableChangeDataFeed = true)
  ```
- **⚠️ NEVER enable CDF on Fivetran-landed tables.** Fivetran manages those destination tables; enabling CDF forces a **full historical re-sync from Fivetran** — billable connector usage you do not want to pay for. Treat every Fivetran-sourced Bronze table as **CDF-off → always full refresh**, and plan the MLVs that read it accordingly. CDF is *only* for the Delta tables you write yourself.
- **Author cross-layer transforms in Spark SQL** (not PySpark) when you want incremental/optimal refresh — see the PySpark limitation under AVOID.
- **Follow [NAMING-CONVENTIONS.md](../../common/NAMING-CONVENTIONS.md)** for every name: snake_case MLVs with no prefix, source-system schemas in Bronze/Silver, business-domain schemas in Gold.
- **Materialize each layer** — Bronze (ingested) → Silver MLVs → Gold MLVs. Do not collapse layers.
- **Set refresh schedules per MLV** aligned to source refresh cadence; let Fabric pick skip/incremental/full.
- **Account for the append-only constraint** when planning ERP/CRM/HRM refresh (see below) — surface it to the user, do not bury it.

### PREFER
- **Spark SQL definitions** over PySpark for any MLV that should refresh incrementally.
- **Managed MLVs in a Lakehouse** over notebook-triggered refresh — you get dependency-aware orchestration, retries, and centralized monitoring; notebook-triggered refresh has none of that.
- **Data-quality constraints in the MLV definition** to drop/flag bad rows on the way into Silver.
- **Incremental-friendly SQL** — the constructs Fabric can refresh incrementally (see below) cover most medallion logic; prefer them over patterns that force full refresh.
- **Direct Lake** semantic models over Gold MLVs (see [MEDALLION-CORE.md § Gold → Power BI](../../common/MEDALLION-CORE.md#gold--power-bi-direct-lake)).

### AVOID
- **PySpark-defined MLVs when you need incremental refresh** — **PySpark MLVs always full-refresh** (optimal refresh for PySpark is "coming soon", not GA). Use Spark SQL for incremental.
- **Non-Delta sources for incremental** — incremental requires Delta sources with CDF; non-Delta sources always full-refresh.
- **⚠️ Enabling CDF on Fivetran-landed Bronze** — never do this. It triggers a costly full re-sync from Fivetran. Fivetran tables must stay CDF-off, so every MLV sourced from them always full-refreshes.
- **Assuming CDF alone guarantees incremental** — if the source records **deletes or updates** between refreshes, Fabric falls back to **full refresh even with CDF enabled and supported SQL**. Incremental applies only when sources are **append-only** between refreshes.
- **External Delta paths as MLV sources** — managed tables only.
- **Hardcoded workspace/lakehouse IDs** — discover via REST API.
- **Pasting full implementations into the skill** — guide the LLM to generate definitions from these patterns.

---

## Refresh Model (the core of MLV)

Fabric applies **optimal refresh**: at each scheduled run it chooses the cheapest valid strategy.

| Strategy | When Fabric chooses it |
|----------|------------------------|
| **Skip** | No changes detected in sources |
| **Incremental** | Sources are Delta + CDF-enabled, **append-only** since last refresh, and the definition uses supported SQL constructs |
| **Full** | Anything else — non-Delta source, no CDF, **Fivetran-landed source (CDF must stay off)**, deletes/updates in source, unsupported construct, or a PySpark-defined MLV |

**Incremental-supported SQL constructs** (GA expansion): aggregations (`count`, `sum`, `group by`),
left outer joins, left semi joins, and common table expressions. Most real-world Silver/Gold logic
qualifies without rewriting.

**Two hard gates for incremental:**
1. **CDF on all source Delta tables** (`delta.enableChangeDataFeed = true`) — **on the tables you land yourself only** (see the Fivetran exception below).
2. **Append-only sources** between refreshes. ERP/CRM/HRM systems frequently update and delete rows — when that happens, expect full refresh regardless of CDF. (Refresh hints to relax this are in private pilot; do not rely on them.) Plan cadence and capacity around occasional full refreshes for mutable sources.

> **⚠️ Fivetran exception — do not miss this.** Fivetran-landed Bronze tables **must not** have CDF enabled: turning it on forces a full, billable re-sync from Fivetran. They therefore **never qualify for incremental refresh** — every MLV that reads a Fivetran source full-refreshes. Enable CDF only on the Delta tables **you** land via `saveAsTable`.

Spark SQL definitions are eligible for optimal/incremental refresh; **PySpark definitions are not** (always full).

---

## Authoring MLVs

Definitions live in a Spark SQL context bound to the target layer's lakehouse. General shape
(verify exact clause syntax — especially data-quality constraints — against the
[Learn docs](#references)):

```sql
create materialized lake view if not exists silver.sales_order_lines
  -- optional data-quality constraint: drop rows that fail
  (
    constraint valid_amount check (amount > 0) on mismatch drop
  )
  comment 'Conformed, deduplicated sales order lines from Business Central'
  as
select order_id
     , order_date
     , customer_id
     , amount
  from bc.sales_order_lines
 where amount is not null
```

Gold aggregation MLV (incremental-friendly — `group by` + `sum`/`count`), SQL formatted with
lowercase keywords, leading commas, and column aliases aligned:

```sql
create materialized lake view if not exists sales.daily_sales_summary
  comment 'Daily sales totals for the executive KPI report'
  as
select order_date
     , customer_id
     , sum(amount)                          as total_amount
     , count(*)                             as order_count
  from silver.sales_order_lines
 group by order_date
        , customer_id
```

Guidance for the LLM when generating definitions:

- Reference sources by `{schema}.{table}` within the layer lakehouse; never hardcode GUIDs or FQDNs.
- Keep one MLV per logical entity/mart; let Fabric's lineage chain Silver→Gold.
- Add DQ constraints in Silver MLVs to enforce conformance on the way in.
- Use the incremental-supported constructs above when refresh cost matters.

---

## Orchestration & Refresh Scheduling

- **Per-MLV schedules** (multi-schedule supported) — set each MLV's refresh cadence to match the upstream source's load schedule.
- **Managed MLVs in a Lakehouse**: Fabric resolves the dependency graph (Bronze→Silver→Gold), refreshes in order, retries, and surfaces monitoring. **Prefer this.**
- **Notebook-triggered refresh** (`refresh materialized lake view ...`) exists but gives **no dependency awareness or centralized visibility** — use only for ad-hoc/manual runs.
- Align Gold refresh to land after Silver completes; the managed dependency graph handles ordering for you.

---

## End-to-End Flow

```
Land Bronze (managed tables + CDF) → Silver MLVs (conform/dedup) → Gold MLVs (aggregate) → Schedule refresh → Power BI Direct Lake
```

1. **Provision** workspaces and lakehouses per [NAMING-CONVENTIONS.md](../../common/NAMING-CONVENTIONS.md) (default: `100_Bronze`/`200_Silver`/`300_Gold`, one lakehouse per layer, source-as-schema).
2. **Land Bronze** as managed Delta tables with ingestion metadata; enable CDF on tables feeding incremental MLVs (see [EDW-SOURCE-INGESTION.md](../../common/EDW-SOURCE-INGESTION.md)).
3. **Create Silver MLVs** — conform, deduplicate, apply DQ constraints; Spark SQL for incremental eligibility.
4. **Create Gold MLVs** — business aggregates/marts in business-domain schemas.
5. **Set refresh schedules** per MLV; prefer managed MLVs for dependency-aware orchestration.
6. **Verify** — confirm MLVs populate and refresh strategy behaves as expected (check it picks incremental for append-only sources, full for mutated ones).
7. **Connect Power BI** to Gold via Direct Lake (see [MEDALLION-CORE.md § Gold → Power BI](../../common/MEDALLION-CORE.md#gold--power-bi-direct-lake)).

---

## Examples

### Example 1: Silver conformance MLV with data quality
**Prompt**: "Create a Silver materialized lake view that cleans Business Central sales orders — drop zero/negative amounts and dedupe."
**What the LLM should generate**: a `create materialized lake view silver.sales_order_lines` Spark SQL definition with a DQ constraint and dedup logic, sourced from the managed `bc.sales_order_lines` Bronze table (CDF enabled).

### Example 2: Gold aggregation with incremental refresh
**Prompt**: "Build a Gold daily sales summary that refreshes incrementally."
**What the LLM should generate**: a `create materialized lake view sales.daily_sales_summary` using `group by` + `sum`/`count` (incremental-supported), confirm CDF on the Silver source, and a per-MLV schedule. Note the append-only caveat if the source mutates.

### Example 3: Diagnose unexpected full refresh
**Prompt**: "My MLV keeps doing full refreshes even though I enabled CDF."
**What the LLM should check**: (0) **is the source a Fivetran-landed table?** → CDF must be off, so it always full-refreshes (and CDF should never be turned on — it forces a billable Fivetran re-sync); (1) is the definition PySpark? → always full; (2) is the source non-Delta? → always full; (3) does the source get **updates/deletes** between refreshes? → append-only violation forces full; (4) does the SQL use an unsupported construct? Guide to the supported-construct list.

### Example 4: ERP source planning
**Prompt**: "Set up MLV medallion for our ERP — lots of updated records daily."
**What the LLM should d