"""Shared in-memory state (Databricks job lifetime).

Why this file exists
--------------------
The pipeline uses a small amount of shared, in-memory state across modules.

Putting that state in a dedicated module makes dependencies explicit and keeps
the rest of the codebase largely functional:

- `identity.py` initializes and caches the identity lookup.
- `transforms.py` caches PT data for the current run.
- `orchestrator.py` initializes the target Delta table once.

Important notes
---------------
- This state is **in-memory only** and lasts for the life of the Python process.
- It is safe for Databricks Jobs where a single driver process runs the pipeline.
- Do not treat these values as durable checkpoints; persistence is done via Delta.
"""

from __future__ import annotations

from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

identity_lookup_table: Optional[DataFrame] = None
pt_data_cache: Optional[DataFrame] = None
target_delta_table: Optional[DeltaTable] = None

# Naming guide:
# - identity_lookup_table: Spark DataFrame loaded from correlation lookup Delta table.
# - pt_data_cache: Spark DataFrame containing PT flags (cached to avoid re-reading).
# - target_delta_table: DeltaTable handle for `patronage_unified` used by merge logic.
