# Patronage_Modularized

This folder contains the Patronage pipeline implemented as a modular Python package designed to run in Databricks.

## Pipeline overview
This pipeline is organized around a few core responsibilities:
- **Table initialization**: ensures the target Patronage Delta table exists and the DMDC checkpoint table is available.
- **Identity lookup**: builds/refreshes an identity correlation lookup table and loads it for downstream joins.
- **Source discovery**: finds inbound SCD/CG files to process using modification timestamps and filename patterns.
- **Transforms**:
  - SCD: change-only transform with PT "sticky Y" handling.
  - CG: eligibility/shape normalization and identity enrichment.
- **SCD Type 2 merge**: expires the prior active record on change and inserts a new active record.
- **Scheduled tasks**:
  - Monthly EDIPI backfill (last Friday of the month).
  - DMDC export on Wed/Fri with an incremental extraction window based on a checkpoint table.

## Public entrypoint
- `run_pipeline(processing_mode, verbose_logging=False)`

## Design goals
- Keep business logic organized by responsibility (discovery, transforms, SCD2 merge, scheduled tasks).
- Keep configuration centralized in `config.py`.
- Keep the runtime assumptions explicit for Databricks (`spark`, `dbutils`, Delta Lake).

## Package layout
- `patronage_modularized/`
  - `config.py` – constants/config + logging helpers
  - `state.py` – shared cached state (identity lookup, PT cache, delta table handle)
  - `identity.py` – identity correlation rebuild + loader
  - `discovery.py` – unprocessed file discovery
  - `transforms.py` – CG + SCD transforms (incl PT logic + fallback)
  - `scd2.py` – SCD Type 2 merge helpers
  - `scheduled.py` – EDIPI backfill scheduling + logic
  - `dmdc.py` – DMDC export generation + checkpoint
  - `orchestrator.py` – table init + main `run_pipeline`

## Module guide (what each file does + functions)
Notes:
- The pipeline is intended to be called via `run_pipeline(...)`.
- Functions prefixed with `_` are internal helpers used by their module.

### `patronage_modularized/__init__.py`
Purpose:
- Package entrypoint; re-exports the public API.

Exports:
- `run_pipeline`

### `patronage_modularized/config.py`
Purpose:
- Centralized constants/configuration (paths, schemas, filters, Spark window specs).
- Logging helper and small utilities used across modules.

Functions:
- `log_message`
- `optimize_delta_table`
- `build_dmdc_record_sql`

### `patronage_modularized/state.py`
Purpose:
- Shared in-memory state for the lifetime of a Databricks job run.
- Holds cached DataFrames / DeltaTable handles to avoid repeated I/O.

Functions:
- (none)

### `patronage_modularized/identity.py`
Purpose:
- Builds and loads the identity correlation lookup table used for ICN/EDIPI enrichment.
- Quarantines ambiguous/duplicate correlations into a separate Delta table.

Functions:
- `_filter_psa_data`
- `_filter_person_data`
- `_filter_institution_data`
- `_join_psa_with_institutions`
- `_identify_duplicate_relationships`
- `_remove_duplicate_correlations`
- `_calculate_latest_correlation_dates`
- `_build_correlation_lookup_table`
- `build_identity_correlation_table`
- `_check_daily_identity_rebuild_needed`
- `initialize_identity_lookup`

### `patronage_modularized/discovery.py`
Purpose:
- Determines which inbound source files should be processed on a run.
- Uses timestamps + filename patterns; supports `rebuild` vs `update` windows.

Functions:
- `_get_last_processed_timestamp`
- `_calculate_time_boundary`
- `discover_unprocessed_files`

### `patronage_modularized/transforms.py`
Purpose:
- Reads raw inbound files and transforms them into a standardized target schema.
- Implements SCD change-only logic (hash-based), PT “sticky Y”, and optional fallback identity.

Functions:
- `get_optimized_identity_for_scd`
- `get_patronage_icn_edipi_fallback`
- `_find_pt_backfill_candidates`
- `get_pt_data_for_mode`
- `transform_caregiver_data`
- `_get_direct_scd_changes`
- `_get_pt_driven_scd_changes`
- `transform_scd_data`
- `_standardize_columns_for_merge`
- `create_and_transform_source_dataframe`
- `initialize_caregiver_seed_data`

### `patronage_modularized/scd2.py`
Purpose:
- Implements the SCD Type 2 merge workflow into the Patronage Delta table.
- Handles join-to-latest-active, change detection (for CG), audit fields, and merge execution.

Functions:
- `join_source_with_target_data`
- `identify_record_changes`
- `_build_change_log_expression`
- `_prepare_records_for_upsert`
- `_prepare_records_for_expiration`
- `create_scd_records_with_audit`
- `execute_delta_merge`
- `execute_scd_pipeline_for_source`

### `patronage_modularized/dmdc.py`
Purpose:
- Generates the DMDC export file on scheduled days.
- Uses a checkpoint table to compute the incremental extraction window.
- Writes files via DBFS FUSE path conversion for pandas compatibility.

Functions:
- `is_dmdc_transfer_day`
- `_to_local_fuse_path`
- `_get_dmdc_last_run_date`
- `_query_dmdc_records`
- `_write_dmdc_file`
- `_update_dmdc_checkpoint`
- `generate_dmdc_transfer_file`

### `patronage_modularized/scheduled.py`
Purpose:
- Monthly scheduled tasks (currently EDIPI backfill).
- Finds active Patronage records missing EDIPI, generates a DMDC-format file, and updates the table.

Functions:
- `is_last_friday_of_month`
- `_get_edipi_backfill_candidates`
- `_generate_edipi_backfill_file`
- `_update_patronage_with_backfilled_edipis`
- `run_edipi_backfill`

### `patronage_modularized/orchestrator.py`
Purpose:
- Orchestrates the end-to-end pipeline lifecycle (init, discover, transform, merge, scheduled tasks).

Functions:
- `reset_patronage_table`
- `initialize_all_tables`
- `process_patronage_data`
- `run_pipeline`

## Running in Databricks
Example (Repo path):

```python
import sys
sys.path.append("/Workspace/Repos/<your repo>/Patronage_Modularized")

from patronage_modularized import run_pipeline
run_pipeline("update", verbose_logging=True)
```

Notes:
- Assumes Databricks runtime globals `spark` and `dbutils`.
- Configuration values are centralized in `config.py`.

---

## New-hire quickstart (day 1)

### Prereqs (what must be true before anything will work)
- You are running on a Databricks cluster with access to the required mounts in `config.py` (for example: `/mnt/ci-vadir-shared/`, `/mnt/ci-carma/landing/`, `/mnt/ci-mvi/...`).
- You have permission to read inbound directories and write to the DMDC export directory.
- You are comfortable working in UTC: the pipeline pins Spark session timezone to UTC, and Python date logic is UTC-anchored.

### Which mode should I run?
- **`update`** (normal daily run): processes new inbound files since the last processed timestamp in the Patronage target table.
- **`rebuild`** (special / operational run): processes historical inbound files starting at each source’s configured `beginning_date`.
  - Warning: `rebuild` performs a full reset of the Patronage target table (drops the table and deletes the underlying Delta storage files).
  - Note: rebuild file discovery intentionally stops at the **end of the previous month** (UTC). This is by design to avoid partial-month boundary churn.

### Minimal commands (copy/paste)

Run the normal daily update:

```python
from patronage_modularized import run_pipeline

run_pipeline("update", verbose_logging=True)
```

Run a rebuild (use intentionally):

```python
from patronage_modularized import run_pipeline

run_pipeline("rebuild", verbose_logging=True)
```

### “Did it work?” checklist
After a successful run, a new hire should sanity-check:
- The target table exists and is being updated (`patronage_unified`).
- Update mode shows “Found N unprocessed files” and processes them (or logs “No unprocessed SCD or CG files found”).
- On Wed/Fri, DMDC export may trigger; when it does:
  - a file named like `PATRONAGE_YYYYMMDD.txt` is written under `DMDC_EXPORT_DIR`
  - a new row is inserted into the DMDC checkpoint table (`dmdc_checkpoint`).

---

## Configuration cheat-sheet (what to edit first)
All values below live in `patronage_modularized/config.py`.

### Key outputs (tables / exports)
- **Patronage target Delta table**
  - `PATRONAGE_TABLE_NAME = "patronage_unified"`
  - `PATRONAGE_TABLE_PATH = "dbfs:/user/hive/warehouse/patronage_unified"`
- **DMDC checkpoint table**
  - `DMDC_CHECKPOINT_TABLE_NAME = "dmdc_checkpoint"`
- **DMDC export directory**
  - `DMDC_EXPORT_DIR = "dbfs/mnt/ci-patronage/dmdc_extracts/combined_export"`
  - The writer converts DBFS paths to a local `/dbfs/...` FUSE path before using pandas.

### Inbound sources (file discovery rules)
Inbound discovery is driven by `PIPELINE_CONFIG`:
- **SCD**
  - `path = "/mnt/ci-vadir-shared/"`
  - `matching_characters = "CPIDODIEX_"`
  - `file_extension = "csv"`
  - `beginning_date = "2024-06-30"`
- **CG**
  - `path = "/mnt/ci-carma/landing/"`
  - `matching_characters = "caregiver"`
  - `file_extension = "csv"`
  - `beginning_date = "2024-12-18 20:00:00"`
  - `seed_file = "dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv"` (used for one-time seed behavior during rebuild)

### Identity correlation (EDIPI/participant_id enrichment)
Identity correlation inputs are defined in `CORRELATION_CONFIG` (MVI-backed paths).
If identity correlation looks “stale” or empty, this is the first place to check for path/permission issues.

---

## Reruns, backfills, and safe reprocessing

### If a daily run failed midway
- It is usually safe to rerun `run_pipeline("update")`. The pipeline is designed around idempotent SCD Type 2 merges and file discovery boundaries.
- If you see repeated work you didn’t expect, confirm the discovery window logic in `patronage_modularized/discovery.py`.

### If a file arrived late
- Update mode uses the **target table’s MAX(SDP_Event_Created_Timestamp)** as the start of the discovery window, and **file modification time** to decide whether a file is in-window.
- Late-arriving files with a new modification time will generally be picked up on the next update run.

### If you need to truly “reprocess history”
- Use `rebuild` mode (and expect it to be slower / more expensive).
- Avoid ad-hoc deletion of the DMDC checkpoint table unless you are intentionally resetting export watermarks.

---

## Troubleshooting (most common issues)

### 1) “No unprocessed files found” but you expect new data
- Check the directory configured in `PIPELINE_CONFIG[<source>]["path"]` exists and is readable by `dbutils.fs.ls`.
- Confirm filename prefix and extension rules (`matching_characters`, `file_extension`).
- Confirm the file’s DBFS modification time is within the computed window (see logs: “Time window for …”).

### 2) DBFS path issues (dbfs:/ vs /dbfs)
- Use `dbfs:/...` with `dbutils.fs.*` operations.
- Use `/dbfs/...` (FUSE path) for Python file I/O (pandas, `open`, `os.path`).
  - DMDC export handles this conversion internally.

### 3) DMDC export shows “NO DATA”
- Confirm today is Wed/Fri (UTC) or that you’re intentionally running outside the normal schedule.
- Confirm `DMDC_ELIGIBILITY_FILTER` in `config.py` matches your expectations.
- Confirm the DMDC checkpoint table exists and contains the expected last-run timestamp.

### 4) Identity correlation is empty / EDIPI is missing
- Check MVI source mounts in `CORRELATION_CONFIG` for permissions and freshness.
- Confirm the identity lookup rebuild ran (see logs from `identity.initialize_identity_lookup`).

### 5) Performance surprises
- Rebuild mode scans large windows by design.
- Update mode is usually bounded, but may still be heavy if many files land at once.
- If you need deeper performance tuning, start by checking the table optimize behavior and cluster sizing rather than changing merge logic.

---

## Monitoring (how we know the pipeline is healthy)
In production, this pipeline is typically paired with a separate Databricks Job/notebook that:
- checks inbound directories for freshness (SCD/CG) and export directories for expected outputs (DMDC)
- checks that key Delta tables exist and have recent updates
- fails the job on critical freshness gaps (so alerts fire)

If you have a monitoring notebook in your workspace repo, document its path/name here so new hires can find it quickly.


