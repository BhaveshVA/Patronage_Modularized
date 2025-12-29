"""Patronage pipeline package (Databricks).

Overview
--------
This package implements the Patronage data pipeline as a small set of focused
modules that run in Databricks.

At a high level the pipeline:
1) Ensures required Delta tables exist (target table + DMDC checkpoint table).
2) Loads/refreshes the identity correlation lookup.
3) Discovers inbound files for each source (SCD and CG) using file modification
	timestamps and filename patterns.
4) Transforms each source into a standardized target schema.
5) Applies SCD Type 2 logic in Delta Lake:
	- Expire the previously-active record when a change occurs.
	- Insert a new active record representing the latest state.
6) Runs scheduled tasks:
	- Monthly EDIPI backfill (last Friday of the month).
	- DMDC export (Wednesday/Friday) with a checkpoint for incremental extracts.

Runtime assumptions
-------------------
- Designed to run inside Databricks where `spark` and `dbutils` are available.
- Uses Delta Lake merge semantics (`delta.tables.DeltaTable`).

Public entrypoint
-----------------
`run_pipeline(processing_mode: str, verbose_logging: bool = False) -> None`
	 Orchestrates ingestion, SCD2 merge, and scheduled tasks.
"""

from .orchestrator import run_pipeline

__all__ = ["run_pipeline"]
