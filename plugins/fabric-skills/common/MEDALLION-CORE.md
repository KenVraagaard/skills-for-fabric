# Medallion Core (Transformation-Agnostic)

Foundational, **transformation-agnostic** medallion content shared by every EDW pattern skill
(`e2e-medallion-mlv`, `e2e-medallion-warehouse`, `e2e-medallion-notebook`). It covers what is
true of a Bronze/Silver/Gold EDW **regardless of the engine** that moves data between layers.
Engine-specific mechanics live in the individual pattern skills.

> **Naming**: all names in this document follow [NAMING-CONVENTIONS.md](./NAMING-CONVENTIONS.md) —
> the canonical authority. Do not invent alternative names.

> **Scope**: batch EDW from business-system sources (ERP/CRM/HRM). Streaming / Real-Time
> Intelligence is out of scope.

---

## The Three Transformation Patterns

All three are **the same medallion architecture**. What differs is only the **transformation
engine between layers** and its orchestration. Pick the pattern per the workload; the layer
concepts, naming, workspace/lakehouse topology, RBAC, and Gold→Power BI consumption below are
identical across all three.

| # | Pattern | Engine between layers | Orchestration | Skill |
|---|---------|-----------------------|---------------|-------|
| 1 | **MLV** | Declarative Spark SQL `CREATE MATERIALIZED LAKE VIEW` | Per-MLV schedules (multi-schedule) | `e2e-medallion-mlv` |
| 2 | **Warehouse T-SQL** | Stored procedures / views in a Fabric Warehouse | Data Factory pipelines | `e2e-medallion-warehouse` *(stub)* |
| 3 | **Notebook ETL** | Imperative PySpark / Spark SQL in notebooks | Apache Airflow Jobs in Fabric | `e2e-medallion-notebook` *(stub)* |

> **Dataflow Gen2 was evaluated and rejected** as a cross-layer pattern — it is not
> parameterizable enough to drive Bronze→Silver→Gold consistently. Acceptable for one-off,
> low-code transforms only.

---

## Layer Concepts

| Layer | Purpose | Schema posture | Typical contents |
|-------|---------|----------------|------------------|
| **Bronze** (Raw) | Land source data exactly as received | Schema-on-read; flexible | Source-native tables + ingestion metadata; full history / audit trail |
| **Silver** (Cleaned) | Deduplicated, validated, conformed business entities | Schema enforcement | Conformed entities keyed on natural/business keys |
| **Gold** (Curated) | Business-approved marts for analytics | Strict governance | Aggregates, dimensional marts, KPI tables for BI |

Principles that hold for **every** pattern:

- **Each layer is physically materialized** as Delta tables — never chain shortcuts Bronze→Silver→Gold. Materialization is what gives lineage, governance, and independent optimization.
- **Bronze keeps everything**: never filter or clean on the way in. Cleaning happens Bronze→Silver.
- **Don't skip Silver.** Bronze→Gold directly loses the conformance/dedup layer that makes Gold trustworthy.
- **Add ingestion metadata in Bronze**: ingestion timestamp, source file/batch ID, source system code. See [EDW-SOURCE-INGESTION.md](./EDW-SOURCE-INGESTION.md).
- **Delta Lake format for all layers.**

---

## Workspace & Lakehouse Topology

Follows [NAMING-CONVENTIONS.md](./NAMING-CONVENTIONS.md).

### Default

- **Three per-layer workspaces**: `100_Bronze_{env}`, `200_Silver_{env}`, `300_Gold_{env}`.
- **One lakehouse per layer**, with each **source system as a schema** inside it
  (`LH_100_Bronze` containing schemas `bc`, `ln`, `jde`, …). Gold uses **business-domain
  schemas** (`sales`, `finance`, `hr`).

### Domain / security-boundary separation

When a domain such as **Finance, HR, or Operations** carries a real isolation requirement, give
it a **dedicated per-layer workspace set** (`FIN_100_Bronze_{env}`, `FIN_200_Silver_{env}`,
`FIN_300_Gold_{env}`) rather than co-mingling it in the shared workspaces. Domains without an
isolation requirement stay in the shared workspaces and are separated by schema. See
[NAMING-CONVENTIONS.md § Workspaces](./NAMING-CONVENTIONS.md#workspaces).

### Single-workspace override

For a POC or small team, a single workspace holding per-layer lakehouses is acceptable. Preserve
layer separation logically (separate lakehouses, separate schemas) and call out the weaker
governance boundary explicitly.

### Cross-layer reads

Silver reads Bronze, and Gold reads Silver, via **cross-workspace OneLake access** using fully
qualified references — discover workspace and lakehouse IDs through the Fabric REST API
(see [COMMON-CORE.md](./COMMON-CORE.md)); never hardcode GUIDs.

---

## RBAC

| Layer / boundary | Who has access | Rights |
|------------------|----------------|--------|
| Bronze | Ingestion / data engineering | Write (land + reprocess) |
| Silver | Data engineering / data quality | Write (transform), read Bronze |
| Gold | Analysts / BI consumers | Read curated marts; engineers write |
| Domain-separated set (FIN/HR/OPS) | Only that domain's authorized members | Per-layer rights scoped to the domain workspace set |

- Grant rights at the **workspace** level for the default topology; at the **schema** level when isolating sources/domains inside a shared lakehouse.
- Prefer Entra ID **security groups** over individual user grants.
- Service principals (not personal accounts) run scheduled refreshes and pipelines; hold secrets in Key Vault or environment variables.

---

## Gold → Power BI (Direct Lake)

Gold is the consumption layer for BI. The connection pattern is the same for all three transformation patterns:

1. **Discover the Gold lakehouse SQL endpoint** via `GET /v1/workspaces/{workspaceId}/lakehouses/{goldLakehouseId}` — read `properties.sqlEndpointProperties.connectionString` and wait for `provisioningStatus = Success`.
2. **Build a Direct Lake semantic model** (`SM_{domain}`) over the Gold tables — reads OneLake Delta directly, no import/duplication. Use the [semantic-model-authoring](../skills/semantic-model-authoring/SKILL.md) skill for TMDL.
3. **Build the report** (`RPT_{audience}_{purpose}`) on the semantic model. Use the Power BI report skills (`powerbi-report-planning` / `-design` / `-authoring`).
4. **Match table/column names exactly** to the Gold Delta tables.
5. **Verify end-to-end** with a DAX query via [semantic-model-consumption](../skills/semantic-model-consumption/SKILL.md).

For V-Order and read-optimization of Gold tables, see the pattern-specific skill (each engine sets these differently).

---

## Companion Documents

- [NAMING-CONVENTIONS.md](./NAMING-CONVENTIONS.md) — canonical naming authority.
- [EDW-SOURCE-INGESTION.md](./EDW-SOURCE-INGESTION.md) — getting ERP/CRM/HRM data into Bronze.
- [COMMON-CORE.md](./COMMON-CORE.md) — Fabric REST API patterns, auth, item discovery.
- [COMMON-CLI.md](./COMMON-CLI.md) — `az rest`, token acquisition, SQL/TDS access.
- [SPARK-AUTHORING-CORE.md](./SPARK-AUTHORING-CORE.md) — notebook/lakehouse creation, job execution.
