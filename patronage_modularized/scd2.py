"""patronage_modularized.scd2

SCD Type 2 merge implementation for the Patronage target Delta table.

This module encapsulates the logic to:
- Join transformed source data to the latest active target records.
- Identify new vs. changed vs. unchanged records.
- Generate an audit log describing changes.
- Execute an SCD2-style merge in Delta:
    - Expire old active records (set `RecordStatus = False`).
    - Insert new active records representing the latest state.

Important behavior note:
The pipeline uses a single merge with an operation key:
- Rows with `SCDType2_OPERATION_KEY` set are matched to expire old records.
- Rows with `SCDType2_OPERATION_KEY` null are inserted as new active records.

Databricks assumptions:
- Uses `delta.tables.DeltaTable` for merge.
- Requires `spark` to create empty DataFrames for pass-through cases.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from databricks.sdk.runtime import *  # noqa: F403
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    asc,
    coalesce,
    col,
    concat_ws,
    desc,
    lit,
    row_number,
    when,
    xxhash64,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from . import config, state
from .config import SPARK_CONFIG, SOURCE_TYPE_SCD, TARGET_TABLE_COLUMN_MAPPING, log_message


def join_source_with_target_data(source_df: DataFrame, target_table: DeltaTable, source_type: str) -> DataFrame:
    """Join a transformed source DataFrame with the latest active target rows.

    Args:
        source_df: Standardized transformed data.
        target_table: DeltaTable handle for the Patronage target.
        source_type: Source identifier.

    Returns:
        DataFrame that includes both source columns and `target_*` columns used
        for change detection.

        Notes:
                - For SCD, `transform_scd_data` already performed target join and
                    change detection, so we bypass this join for efficiency.
                - For CG, we add a NULL `Status_Last_Update` to keep a consistent schema.
    """
    log_message(
        f"Joining {source_type} source data with target table (with de-duplication)...",
        level="DEBUG",
        depth=2,
    )

    cfg = SPARK_CONFIG[source_type]

    # For SCD, transform_scd_data already performed the target join and pre-filtered for changes.
    if source_type == SOURCE_TYPE_SCD:
        log_message("Bypassing target join for SCD as new transform handles it.", level="DEBUG", depth=3)
        return source_df

    target_join_keys = cfg["target_join_keys"]
    window_spec = Window.partitionBy(*[col(f"target_{key}") for key in target_join_keys]).orderBy(
        desc("target_SDP_Event_Created_Timestamp")
    )

    target_df = target_table.toDF()
    latest_target_df = (
        target_df.filter((col("Batch_CD") == source_type) & col("RecordStatus"))
        .select([col(c).alias(f"target_{c}") for c in target_df.columns])
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    join_conditions = cfg["join_conditions"]
    joined_df = source_df.join(latest_target_df, join_conditions, "leftouter")

    # For CG, add null Status_Last_Update for a consistent schema
    return joined_df.withColumn("Status_Last_Update", lit(None).cast(StringType()))


def identify_record_changes(dataframe: DataFrame, source_type: str) -> Tuple[DataFrame, DataFrame]:
    """Split joined rows into changed vs unchanged sets.

    Args:
        dataframe: Output of `join_source_with_target_data`.
        source_type: Source identifier.

    Returns:
        (changed_records, unchanged_records)

    Notes:
        For SCD, the transform already produced only changed rows, so we return
        the full dataframe as `changed_records` and an empty dataframe as
        `unchanged_records`.
    """
    log_message("Identifying record changes...", level="DEBUG", depth=3)

    # For SCD, transform_scd_data already produced only changed rows.
    if source_type == SOURCE_TYPE_SCD:
        log_message("SCD changes pre-identified by new transform. Passing through.", level="DEBUG", depth=4)
        unchanged_records = spark.createDataFrame([], dataframe.schema)  # noqa: F405
        return dataframe, unchanged_records

    delta_conditions = SPARK_CONFIG[source_type]["delta_conditions"]
    changed_records = dataframe.filter(delta_conditions)
    unchanged_records = dataframe.filter(~delta_conditions)
    return changed_records, unchanged_records


def _build_change_log_expression(source_type: str) -> Any:
    """Build a Spark expression describing column-level changes.

    Returns:
        A Spark expression that concatenates change descriptions.

    Special rule:
        For SCD PT indicator, we only log N->Y transitions to reflect the
        business "sticky Y" behavior.
    """
    change_conditions = []

    for src_col, tgt_col in SPARK_CONFIG[source_type]["columns_to_track"]:
        if src_col == "PT_Indicator" and source_type == SOURCE_TYPE_SCD:
            condition = when(
                (coalesce(col(tgt_col), lit("N")) == "N") & (col(src_col) == "Y"),
                "PT_Indicator old value: N changed to new value: Y",
            )
        else:
            condition = when(
                xxhash64(coalesce(col(src_col), lit("Null")))
                != xxhash64(coalesce(col(tgt_col), lit("Null"))),
                concat_ws(
                    " ",
                    lit(src_col),
                    lit("old value:"),
                    coalesce(col(tgt_col), lit("Null")),
                    lit("changed to new value:"),
                    coalesce(col(src_col), lit("Null")),
                ),
            )
        change_conditions.append(condition)

    return concat_ws(" ", *[coalesce(c, lit("")) for c in change_conditions])


def _prepare_records_for_upsert(audited_records: DataFrame, source_type: str) -> DataFrame:
    """Mark records for insertion as new active rows in the merge."""
    return audited_records.withColumn("SCDType2_OPERATION_KEY", lit(None).cast(StringType()))


def _prepare_records_for_expiration(audited_records: DataFrame, source_type: str) -> DataFrame:
    """Mark records that should expire existing active target rows.

    The merge logic uses `SCDType2_OPERATION_KEY` as the match key for expiring
    previously-active rows.
    """
    expiration_condition = SPARK_CONFIG[source_type]["expiration_condition"]

    if source_type == SOURCE_TYPE_SCD:
        key_column = col("participant_id")
    else:
        key_column = SPARK_CONFIG[source_type]["concat_column"]

    return audited_records.filter(expiration_condition).withColumn("SCDType2_OPERATION_KEY", key_column)


def create_scd_records_with_audit(changed_records: DataFrame, source_type: str) -> DataFrame:
    """Attach audit metadata to changed records.

    Adds:
        - `RecordChangeStatus`: "New Record" vs "Updated Record"
        - `change_log`: human-readable change description
    """
    change_log_expr = _build_change_log_expression(source_type)
    return changed_records.withColumn(
        "RecordChangeStatus",
        when(col("target_RecordStatus").isNull(), "New Record").otherwise("Updated Record"),
    ).withColumn("change_log", change_log_expr)


def execute_delta_merge(final_df: DataFrame, target_table: DeltaTable, source_type: str) -> None:
    """Execute the Delta merge implementing SCD2 expire/insert.

    Args:
        final_df: Union of rows to expire + rows to insert.
        target_table: DeltaTable handle to merge into.
        source_type: Source identifier (affects merge conditions).

    Side effects:
        Updates existing active records to `RecordStatus = False` and inserts
        new active rows.
    """
    log_message("Executing Delta merge operation...", depth=2)

    initial_record_count = -1
    if config.LOGGING_VERBOSE:
        records_to_expire = final_df.filter(col("SCDType2_OPERATION_KEY").isNotNull()).count()
        records_to_insert = final_df.filter(col("SCDType2_OPERATION_KEY").isNull()).count()
        log_message(f"Records to insert (new active): {records_to_insert:,}", depth=3)
        log_message(f"Records to expire (update old): {records_to_expire:,}", depth=3)
        initial_record_count = target_table.toDF().count()

    merge_conditions = SPARK_CONFIG[source_type]["merge_conditions"]

    try:
        (
            target_table.alias("target")
            .merge(final_df.alias("source"), merge_conditions)
            .whenMatchedUpdate(
                set={
                    "RecordStatus": "False",
                    "RecordLastUpdated": "to_date(source.SDP_Event_Created_Timestamp)",
                    "sentToDoD": "true",
                    "RecordChangeStatus": lit("Expired Record"),
                }
            )
            .whenNotMatchedInsert(values=TARGET_TABLE_COLUMN_MAPPING)
            .execute()
        )
    except Exception as e:
        log_message(f"ERROR: Delta merge operation failed: {str(e)}")
        raise

    if config.LOGGING_VERBOSE:
        final_record_count = target_table.toDF().count()
        log_message(
            f"Table record count changed from {initial_record_count:,} to {final_record_count:,} "
            f"(Delta: {final_record_count - initial_record_count:,})",
            depth=3,
        )


def execute_scd_pipeline_for_source(transformed_df: Optional[DataFrame], source_type: str) -> None:
    """Run SCD2 merge workflow for a single source type.

    Args:
        transformed_df: Output of transforms (already standardized), or None.
        source_type: Source identifier.

    Behavior:
        - Skips when there are no rows.
        - Produces:
          - expire rows (to close out previous active record)
          - insert rows (new active record for current state)
        - Executes a Delta merge.
    """
    if transformed_df is None:
        log_message(
            f"No transformed data to process for {source_type}. Skipping SCD pipeline.",
            level="DEBUG",
            depth=2,
        )
        return

    transformed_count = transformed_df.count()
    if transformed_count == 0:
        log_message(f"No transformed data to process for {source_type}. Skipping SCD pipeline.", depth=1)
        return

    log_message(f"Starting {source_type.upper()} pipeline for {transformed_count:,} records...", depth=1)

    if state.target_delta_table is None:
        raise RuntimeError("Target Delta table is not initialized. Call initialize_all_tables() first.")

    joined_df = join_source_with_target_data(transformed_df, state.target_delta_table, source_type)
    changed_records, unchanged_records = identify_record_changes(joined_df, source_type)

    changed_records.cache()

    if config.LOGGING_VERBOSE:
        changed_count = changed_records.count()
        unchanged_count = unchanged_records.count()
        log_message(f"Change detection: {changed_count:,} changed, {unchanged_count:,} unchanged.", depth=2)
    else:
        changed_count = changed_records.count()

    if changed_count == 0:
        log_message("No changes to merge. Pipeline step complete.", depth=2)
        return

    updates = changed_records.filter(col("target_RecordStatus").isNotNull())
    new_records = changed_records.filter(col("target_RecordStatus").isNull())

    expire_rows = _prepare_records_for_expiration(create_scd_records_with_audit(updates, source_type), source_type)
    insert_rows_from_updates = _prepare_records_for_upsert(create_scd_records_with_audit(updates, source_type), source_type)
    insert_rows_from_new = _prepare_records_for_upsert(create_scd_records_with_audit(new_records, source_type), source_type)

    final_df = expire_rows.unionByName(insert_rows_from_updates).unionByName(insert_rows_from_new)

    execute_delta_merge(final_df, state.target_delta_table, source_type)

    changed_records.unpersist()
    log_message(f"SCD pipeline for {source_type} completed successfully.", depth=1)
