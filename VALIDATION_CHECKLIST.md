# Patronage Pipeline – Validation Checklist

This checklist validates the behavior and outputs of the `patronage_modularized` pipeline.

## 1) Smoke checks
- Confirm imports work in Databricks:
  - `import patronage_modularized as pipeline`
  - `pipeline.run_pipeline('update', verbose_logging=False)`
- Confirm all required tables exist:
  - `patronage_unified`
  - `dmdc_checkpoint`
  - `correlation_lookup`
  - `patronage_pipeline_log`

## 2) Patronage table checks (high-level)
- Row counts (total + active):
  - `SELECT COUNT(*) FROM patronage_unified`
  - `SELECT COUNT(*) FROM patronage_unified WHERE RecordStatus = true`
- Active counts by source:
  - `SELECT Batch_CD, COUNT(*) FROM patronage_unified WHERE RecordStatus = true GROUP BY Batch_CD ORDER BY Batch_CD`

## 3) SCD2 behavior checks
- Pick a small set of keys that changed today (SCD and CG) and verify:
  - A prior active record was expired: `RecordStatus=false`, `RecordChangeStatus='Expired Record'`
  - A new active record exists with updated values and `RecordChangeStatus='Updated Record'` (or `New Record`)
- Verify `change_log` is populated for updated records.

## 4) EDIPI backfill checks
- Before backfill:
  - `SELECT COUNT(*) FROM patronage_unified WHERE RecordStatus=true AND edipi IS NULL`
- After a backfill run:
  - Count decreases and updated rows have `edipi` set.
- Confirm the backfill file exists under `DMDC_EXPORT_DIR`.

## 5) DMDC export checks
- Run only on Wed/Fri:
  - `is_dmdc_transfer_day()` should match expected calendar behavior.
- Confirm export contents:
  - Output filename: `PATRONAGE_YYYYMMDD.txt`
  - Newlines are Unix (`\n`)
  - Sort order: `Batch_CD`, then `edipi`
- Check checkpoint insert:
  - `SELECT * FROM dmdc_checkpoint ORDER BY checkpoint_timestamp DESC LIMIT 5`

## 6) Performance sanity
- Verify no unexpected full scans on daily update runs.
- Confirm identity lookup refresh runs (daily rebuild behavior).

## 7) Pipeline log checks
- Confirm a log row is written per run:
  - `SELECT * FROM patronage_pipeline_log ORDER BY run_timestamp_utc DESC LIMIT 5`
- Confirm status and counts are populated for the latest run.
