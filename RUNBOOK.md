# Patronage Pipeline Runbook (Databricks)

This runbook is operational guidance for running and troubleshooting the `patronage_modularized` pipeline in Databricks.

Related docs:
- Architecture: `HIGH_LEVEL_ARCHITECTURE.md`
- Validation checklist: `VALIDATION_CHECKLIST.md`
- Developer overview: `README.md`

---

## 0) Quick context

The pipeline:
- Discovers inbound source files (SCD + CG) from mount paths configured in `patronage_modularized/config.py`.
- Transforms + deduplicates data and performs SCD Type 2 merges into the Patronage Delta table.
- Runs scheduled tasks:
  - DMDC export on Wed/Fri (incremental window based on a checkpoint table)
  - Monthly EDIPI backfill on the last Friday of the month
  - Monthly deep-clone backups on the first day of the month

Important invariants:
- UTC is the source of truth. The pipeline anchors Spark session timezone and Python date logic to UTC.
- DBFS paths:
  - Use `dbfs:/...` with `dbutils.fs.*` (mkdirs/ls/rm)
  - Use `/dbfs/...` for Python file I/O (pandas writes)

---

## 1) Preflight checks (run before production)

Run these in a Databricks notebook attached to the same cluster used by the job.

### 1.1 Confirm config values (DMDC)

```python
from patronage_modularized.config import DMDC_CHECKPOINT_TABLE_NAME, DMDC_EXPORT_DIR
print("DMDC_CHECKPOINT_TABLE_NAME =", DMDC_CHECKPOINT_TABLE_NAME)
print("DMDC_EXPORT_DIR =", DMDC_EXPORT_DIR)
```

Expected (production):
- `DMDC_CHECKPOINT_TABLE_NAME = dmdc_checkpoint`
- `DMDC_EXPORT_DIR = dbfs:/mnt/ci-patronage/dmdc_extracts/combined_export`

### 1.2 Confirm export directory exists and is writable

```python
from patronage_modularized.config import DMDC_EXPORT_DIR

# Ensure directory exists
_ = dbutils.fs.mkdirs(DMDC_EXPORT_DIR)

# Write a tiny probe file
probe_path = f"{DMDC_EXPORT_DIR}/_write_probe.txt"
dbutils.fs.put(probe_path, "ok\n", overwrite=True)

# Clean up probe file
_ = dbutils.fs.rm(probe_path)
print("Export dir OK:", DMDC_EXPORT_DIR)
```

If this fails:
- You likely have mount/permissions issues, or an incorrect DBFS URI form.
- Confirm you’re using `dbfs:/mnt/...` (note the colon).

### 1.3 Confirm checkpoint table exists and is readable

```sql
-- Run in a SQL cell
SELECT
  max(checkpoint_timestamp) AS last_checkpoint,
  count(*) AS total_rows
FROM dmdc_checkpoint;
```

If the table does not exist, the orchestrator will create it on run.
If this query fails due to permissions, the export will fail as well.

### 1.4 Confirm Patronage target table exists

```sql
SELECT
  count(*) AS total_rows,
  sum(CASE WHEN RecordStatus = true THEN 1 ELSE 0 END) AS active_rows
FROM patronage_unified;
```

### 1.5 Confirm pipeline log table exists

```sql
SELECT
  count(*) AS total_rows,
  max(run_timestamp_utc) AS last_run
FROM patronage_pipeline_log;
```

---

## 2) How to run the pipeline

### 2.1 Update mode (normal daily run)

```python
from patronage_modularized import run_pipeline

run_pipeline("update", verbose_logging=False)
```

Use `verbose_logging=True` when diagnosing unexpected counts or performance.

### 2.2 Rebuild mode (destructive)

Rebuild is an operational tool, not a daily action.

```python
from patronage_modularized import run_pipeline

run_pipeline("rebuild", verbose_logging=False)
```

Notes:
- Rebuild drops the Patronage target table and deletes the underlying Delta files.
- Rebuild file discovery intentionally stops at the end of the previous month (UTC).

---

## 3) Expected outputs and “did it work?”

### 3.1 Discovery expectations

In logs you should see, per source type:
- A time window (start -> boundary)
- “Found N unprocessed files…” or “No unprocessed files…”

### 3.2 Patronage table expectations

After processing:
- New rows inserted or existing rows expired + replaced (SCD2)
- Active counts by source type should look reasonable:

```sql
SELECT Batch_CD, count(*)
FROM patronage_unified
WHERE RecordStatus = true
GROUP BY Batch_CD
ORDER BY Batch_CD;
```

### 3.3 DMDC export expectations (Wed/Fri)

When the DMDC export runs:
- A file `PATRONAGE_YYYYMMDD.txt` is written under the configured export directory
- A new row is inserted into the checkpoint table

Checkpoint verification:

```sql
SELECT *
FROM dmdc_checkpoint
ORDER BY checkpoint_timestamp DESC
LIMIT 10;
```

### 3.4 Monthly EDIPI backfill expectations (last Friday)

- File written: `LAGGED_EDIPI_PATRONAGE_YYYYMMDD.txt` under `DMDC_EXPORT_DIR`
- Patronage table updated to populate `edipi` for eligible active rows

### 3.5 Monthly backups expectations (first day of month)

- Deep-clone snapshots are written to the configured backup locations:
  - `PATRONAGE_BACKUP_DIR`
  - `DMDC_CHECKPOINT_BACKUP_DIR`
  - `PATRONAGE_PIPELINE_LOG_BACKUP_DIR`

#### 3.5.1 Restore from monthly backups (corruption recovery)

- The backups are intended for table restore if data becomes corrupted.
- Restore the affected table from its backup location.
- After restore, run `run_pipeline("update")` so the pipeline reprocesses files from the first day of that month.

Example restore SQL (deep clone from backup paths):

```sql
CREATE OR REPLACE TABLE patronage_unified
DEEP CLONE delta.`dbfs:/mnt/ci-patronage/backups/patronage_unified`;

CREATE OR REPLACE TABLE dmdc_checkpoint
DEEP CLONE delta.`dbfs:/mnt/ci-patronage/backups/dmdc_checkpoint`;

CREATE OR REPLACE TABLE patronage_pipeline_log
DEEP CLONE delta.`dbfs:/mnt/ci-patronage/backups/patronage_pipeline_log`;
```

### 3.6 Pipeline log expectations

- A new row is inserted into `patronage_pipeline_log` per run.

```sql
SELECT *
FROM patronage_pipeline_log
ORDER BY run_timestamp_utc DESC
LIMIT 5;
```

---

## 4) Common failures and fixes

### 4.1 DBFS path issues (most common)

Symptoms:
- `dbutils.fs.ls(...)` fails even though the mount exists
- `mkdirs` fails or pandas writes fail

Fix:
- Use `dbfs:/mnt/...` for `dbutils.fs.*`
- For pandas/local writes, convert to `/dbfs/...` (the pipeline already does this)

### 4.2 Permission failures (mount/table)

Symptoms:
- Errors referencing `/mnt/...` paths
- “permission denied”, “unauthorized”, or similar exceptions

Fix:
- Confirm the cluster/service principal has read access to inbound mounts and write access to export mounts.
- Confirm the job uses the intended cluster/policy.

### 4.3 Checkpoint table issues (DMDC)

Symptoms:
- Export fails when reading or writing checkpoint rows

Fix:
- Verify the table exists and is readable:

```sql
SELECT count(*) FROM dmdc_checkpoint;
```

- If it must be re-created (rare), coordinate with data owners before dropping tables.

### 4.4 Empty DMDC export file

Symptoms:
- File is generated but contains 0 records

Likely causes:
- No eligible active Patronage rows in the extraction window
- Eligibility filter excludes the current population

Triage:
- Compare active Patronage counts and check whether EDIPI is present for the population.

---

## 5) Operational safety notes

- Prefer rerunning `update` after transient failures; the SCD2 merge approach is designed to be idempotent for typical reruns.
- Use `rebuild` only with explicit intent and awareness of the destructive reset.
- Treat DMDC export + checkpointing as an auditable workflow: verify window boundaries before making manual interventions.

---

## 6) Where to change configuration

All operational knobs live in `patronage_modularized/config.py`, including:
- Source discovery inputs (`PIPELINE_CONFIG`)
- Table names + paths
- DMDC checkpoint table name
- DMDC export directory
