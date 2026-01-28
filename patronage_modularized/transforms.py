"""patronage_modularized.transforms

Source-to-target transformations for the Patronage pipeline.

This module converts raw inbound files (SCD and Caregiver) into a standardized
DataFrame ready for SCD Type 2 merge processing.

Key behaviors:
- SCD transformation includes "PT sticky Y": once PT_Indicator is "Y" for a
    participant, it remains "Y" on subsequent updates.
- For SCD, we avoid expensive full-table comparisons by:
    - Deduplicating the source per participant.
    - Joining to active target records.
    - Hashing key attributes to detect changes.
    - Producing only changed rows.
- Optional fallback identity: if the identity correlation table lacks ICN/EDIPI
    for a participant, we attempt to reuse the ICN/EDIPI from an existing active
    Patronage SCD record.

Databricks assumptions:
- Requires `spark` (Databricks Runtime) and the metadata columns supported by
    Spark's file sources (`_metadata.file_path`, `_metadata.file_modification_time`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from databricks.sdk.runtime import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    date_format,
    desc,
    lpad,
    lit,
    row_number,
    substring,
    to_date,
    to_timestamp,
    when,
    xxhash64,
)
from pyspark.sql.types import StringType

from . import config, state
from .config import (
    PIPELINE_CONFIG,
    PATRONAGE_TABLE_NAME,
    SOURCE_TYPE_CG,
    SOURCE_TYPE_SCD,
    SPARK_CONFIG,
    SPARK_DATETIME_FORMAT,
    SPARK_DATE_FORMAT,
    SPARK_DATE_INPUT_FORMAT,
    log_message,
)


def get_optimized_identity_for_scd(identity_df: Optional[DataFrame], participant_ids_df: DataFrame) -> Optional[DataFrame]:
    """Reduce the identity lookup to just the participants that changed.

    Args:
        identity_df: Identity lookup DataFrame (may be None if not initialized).
        participant_ids_df: Distinct participant IDs to retain.

    Returns:
        A DataFrame join result containing only needed identity rows, or None.
    """
    if identity_df is None:
        return None

    return identity_df.join(broadcast(participant_ids_df), "participant_id", "inner")


def get_patronage_icn_edipi_fallback(participant_ids_df: DataFrame) -> Optional[DataFrame]:
    """Fetch fallback ICN/EDIPI values from existing active Patronage SCD rows.

    This supports an edge case where the identity correlation table does not
    return ICN/EDIPI for a participant, but the target table already has those
    values from a previous run.

    Args:
        participant_ids_df: DataFrame containing `participant_id`.

    Returns:
        DataFrame with `participant_id`, `patronage_icn`, `patronage_edipi`, or
        None if unavailable.
    """
    try:
        patronage_active = (
            spark.table(PATRONAGE_TABLE_NAME)
            .filter(col("RecordStatus") & (col("Batch_CD") == SOURCE_TYPE_SCD))
            .select(
                "participant_id",
                col("ICN").alias("patronage_icn"),
                col("edipi").alias("patronage_edipi"),
            )
        )

        return (
            participant_ids_df.join(patronage_active, "participant_id", "left")
            .select("participant_id", "patronage_icn", "patronage_edipi")
        )
    except Exception as e:
        log_message(
            f"Warning: Could not retrieve fallback ICN/EDIPI from Patronage table: {str(e)}",
            level="WARN",
            depth=2,
        )
        return None


def _find_pt_backfill_candidates(pt_data: DataFrame, target_active: DataFrame, deduped_source: DataFrame) -> DataFrame:
    """Identify participants who require a PT-driven update.

    PT-driven updates are needed when:
    - PT delta shows PT="Y"
    - The participant has an active target record
    - The participant is *not* already present in the current SCD source file
      batch (so we'd otherwise miss the PT flip)
    - The target PT_Indicator is not already "Y" (sticky Y rule)
    """
    pt_y_records = pt_data.filter(col("pt_indicator_value") == "Y")
    candidates_in_target = pt_y_records.join(target_active, "participant_id", "inner")
    pt_driven_updates = candidates_in_target.join(deduped_source, "participant_id", "left_anti")

    return pt_driven_updates.filter(coalesce(col("target_PT_Indicator"), lit("N")) != "Y")


def get_pt_data_for_mode(processing_mode: str) -> DataFrame:
    """Load PT indicator data according to processing mode.

    Args:
        processing_mode: "rebuild" loads seed + delta, "update" loads delta only.

    Returns:
        Cached DataFrame with columns: `participant_id`, `pt_indicator_value`.

    Notes:
        The result is cached in `state.pt_data_cache` to avoid re-reading
        datasets during a run.
    """
    pt_config = PIPELINE_CONFIG[SOURCE_TYPE_SCD]["pt_sources"]

    if state.pt_data_cache is not None:
        log_message("Using cached PT data...", level="DEBUG", depth=2)
        return state.pt_data_cache

    if processing_mode == "rebuild":
        log_message("Loading and caching PT historical data (seed + delta)...", level="DEBUG", depth=2)
        pt_seed_data = (
            spark.read.format("delta")
            .load(pt_config["seed_file"])
            .selectExpr("PTCPNT_VET_ID as participant_id", "PT_35_FLAG as pt_indicator_value")
        )
        pt_delta_data = (
            spark.read.format("delta")
            .load(pt_config["delta_table"])
            .selectExpr("PTCPNT_VET_ID as participant_id", "PT_35_FLAG as pt_indicator_value")
        )

        pt_data = (
            pt_seed_data.union(pt_delta_data)
            .filter(col("pt_indicator_value").isNotNull())
            .dropDuplicates(["participant_id"])
        )
        state.pt_data_cache = pt_data.cache()
    else:
        log_message("Loading and caching PT delta data for update...", level="DEBUG", depth=2)
        pt_data = (
            spark.read.format("delta")
            .load(pt_config["delta_table"])
            .selectExpr("PTCPNT_VET_ID as participant_id", "PT_35_FLAG as pt_indicator_value")
            .filter(col("pt_indicator_value").isNotNull())
        )
        state.pt_data_cache = pt_data.cache()

    if config.LOGGING_VERBOSE:
        log_message(f"PT data prepared and broadcasted: {state.pt_data_cache.count():,} records", depth=2)

    return state.pt_data_cache


def transform_caregiver_data(raw_df: DataFrame) -> DataFrame:
    """Transform Caregiver (CG) raw file rows into target-ready shape.

    Args:
        raw_df: Raw Caregiver DataFrame including metadata columns.

    Returns:
        DataFrame containing standardized CG fields plus identity columns
        joined from `state.identity_lookup_table`.

    Raises:
        RuntimeError: if identity lookup is not initialized.
    """
    log_message("Transforming Caregiver data...", level="DEBUG", depth=2)
    source_config = PIPELINE_CONFIG[SOURCE_TYPE_CG]
    spark_config = SPARK_CONFIG[SOURCE_TYPE_CG]

    deduplicated_df = (
        raw_df.filter(col("Caregiver_ICN__c").isNotNull())
        .withColumn("rn", row_number().over(spark_config["window_spec"]))
        .filter(col("rn") == 1)
        .drop("rn", "CreatedDate")
    )

    transformed_df = deduplicated_df.select(
        substring("Caregiver_ICN__c", 1, 10).alias("ICN"),
        substring("Veteran_ICN__c", 1, 10).alias("Veteran_ICN"),
        date_format(to_date("Benefits_End_Date__c", source_config["input_date_format"]), source_config["output_date_format"]).alias(
            "Status_Termination_Date"
        ),
        col("Applicant_Type__c").alias("Applicant_Type"),
        col("Caregiver_Status__c").alias("Caregiver_Status"),
        date_format(to_date("Dispositioned_Date__c", source_config["input_date_format"]), source_config["output_date_format"]).alias(
            "Status_Begin_Date"
        ),
        "filename",
        "SDP_Event_Created_Timestamp",
    )

    if state.identity_lookup_table is None:
        raise RuntimeError("Identity lookup table is not initialized. Call initialize_identity_lookup() first.")

    return transformed_df.join(state.identity_lookup_table, "ICN", "left")


def _get_direct_scd_changes(
    deduped_source: DataFrame,
    pt_data: DataFrame,
    target_active: DataFrame,
    source_config: Dict[str, Any],
    current_filename: str,
    current_timestamp: datetime,
) -> DataFrame:
    """Compute SCD changes driven directly by the SCD source file.

    Produces candidate rows where either:
    - There is no existing active target record, or
    - The hash of tracked attributes differs from the active target.

    PT sticky behavior:
        `PT_Indicator` is computed such that it is "Y" if either target is "Y"
        or PT delta indicates "Y".
    """
    source_pt_target_joined = (
        deduped_source.join(broadcast(pt_data), "participant_id", "left").join(target_active, "participant_id", "left")
    )

    calculated_df = (
        source_pt_target_joined.withColumn(
            "SC_Combined_Disability_Percentage",
            lpad(
                coalesce(
                    col("source_SC_Combined_Disability_Percentage"),
                    col("target_SC_Combined_Disability_Percentage"),
                    lit("000"),
                ),
                3,
                "0",
            ),
        )
        .withColumn(
            "PT_Indicator",
            when(col("target_PT_Indicator") == "Y", "Y").when(col("pt_indicator_value") == "Y", "Y").otherwise("N"),
        )
        .withColumn(
            "Status_Last_Update",
            date_format(col("source_DSBL_DTR_DT_parsed"), source_config["output_date_format"]),
        )
        .withColumn(
            "Status_Begin_Date",
            coalesce(
                col("target_Status_Begin_Date"),
                date_format(col("source_DSBL_DTR_DT_parsed"), source_config["output_date_format"]),
            ),
        )
    )

    hashed_df = calculated_df.withColumn(
        "hash_source", xxhash64("SC_Combined_Disability_Percentage", "PT_Indicator")
    ).withColumn("hash_target", xxhash64("target_SC_Combined_Disability_Percentage", "target_PT_Indicator"))

    changed_df = hashed_df.filter((col("target_RecordStatus").isNull()) | (col("hash_source") != col("hash_target")))

    return changed_df.select(
        "participant_id",
        "SC_Combined_Disability_Percentage",
        "PT_Indicator",
        "Status_Last_Update",
        "Status_Begin_Date",
        lit(current_filename).alias("filename"),
        lit(current_timestamp).alias("SDP_Event_Created_Timestamp"),
        "target_SC_Combined_Disability_Percentage",
        "target_PT_Indicator",
        "target_RecordStatus",
    )


def _get_pt_driven_scd_changes(
    deduped_source: DataFrame,
    pt_data: DataFrame,
    target_active: DataFrame,
    current_filename: str,
    current_timestamp: datetime,
) -> DataFrame:
    """Compute SCD changes driven solely by PT delta (backfill updates)."""
    pt_driven_needed = _find_pt_backfill_candidates(pt_data, target_active, deduped_source)

    return pt_driven_needed.select(
        "participant_id",
        col("target_SC_Combined_Disability_Percentage").alias("SC_Combined_Disability_Percentage"),
        lit("Y").alias("PT_Indicator"),
        col("target_Status_Last_Update").alias("Status_Last_Update"),
        col("target_Status_Begin_Date").alias("Status_Begin_Date"),
        lit(current_filename).alias("filename"),
        lit(current_timestamp).alias("SDP_Event_Created_Timestamp"),
        "target_SC_Combined_Disability_Percentage",
        "target_PT_Indicator",
        "target_RecordStatus",
    )


def transform_scd_data(raw_df: DataFrame, pt_data: DataFrame, processing_mode: str) -> Optional[DataFrame]:
    """Transform SCD raw file rows into a change-only DataFrame.

    Args:
        raw_df: Raw SCD DataFrame read from CSV (with Spark metadata columns).
        pt_data: PT delta/seed DataFrame from `get_pt_data_for_mode`.
        processing_mode: "rebuild" or "update" (used for PT data selection).

    Returns:
        DataFrame containing only changed participants ready for merge, or
        None when the batch has no usable rows.

    Notes:
        The returned DataFrame includes `target_*` columns used downstream by
        the SCD2 merge logic to expire/insert records.
    """
    log_message("Transforming SCD data using Optimized Hash-Based approach...", level="DEBUG", depth=2)
    source_config = PIPELINE_CONFIG[SOURCE_TYPE_SCD]
    spark_config = SPARK_CONFIG[SOURCE_TYPE_SCD]

    try:
        meta_row = raw_df.select("_metadata.file_path", "_metadata.file_modification_time").first()
    except Exception:
        meta_row = None

    if meta_row is None:
        log_message(
            "Warning: Could not extract metadata from source file (file might be empty). Skipping this batch.",
            level="WARN",
        )
        return None

    current_filename = meta_row.file_path
    current_timestamp = meta_row.file_modification_time

    raw_source_records = (
        raw_df.selectExpr(
            "PTCPNT_ID as participant_id",
            "CMBNED_DEGREE_DSBLTY as source_SC_Combined_Disability_Percentage",
            "DSBL_DTR_DT as source_DSBL_DTR_DT",
        )
        .withColumn(
            "source_DSBL_DTR_DT_parsed",
            to_date(col("source_DSBL_DTR_DT"), source_config["input_date_format"]),
        )
        .filter(col("source_DSBL_DTR_DT_parsed").isNotNull() & col("participant_id").isNotNull())
    )

    deduped_source = (
        raw_source_records.withColumn("rn", row_number().over(spark_config["source_dedup_window_spec"]))
        .filter(col("rn") == 1)
        .drop("rn")
    ).cache()

    target_active = (
        spark.table(PATRONAGE_TABLE_NAME)
        .filter(col("RecordStatus") & (col("Batch_CD") == SOURCE_TYPE_SCD))
        .select(
            "participant_id",
            col("SC_Combined_Disability_Percentage").alias("target_SC_Combined_Disability_Percentage"),
            col("PT_Indicator").alias("target_PT_Indicator"),
            col("Status_Begin_Date").alias("target_Status_Begin_Date"),
            col("Status_Last_Update").alias("target_Status_Last_Update"),
            col("RecordStatus").alias("target_RecordStatus"),
        )
    ).cache()

    direct_changes = _get_direct_scd_changes(deduped_source, pt_data, target_active, source_config, current_filename, current_timestamp)
    pt_driven_changes = _get_pt_driven_scd_changes(deduped_source, pt_data, target_active, current_filename, current_timestamp)

    all_changes = direct_changes.unionByName(pt_driven_changes)
    changed_participants = all_changes.select("participant_id").distinct()

    filtered_identity = get_optimized_identity_for_scd(state.identity_lookup_table, changed_participants)
    if filtered_identity is None:
        raise RuntimeError("Identity lookup table is not initialized. Call initialize_identity_lookup() first.")
    patronage_fallback = get_patronage_icn_edipi_fallback(changed_participants)

    final_df = all_changes.join(filtered_identity, "participant_id", "left")

    if patronage_fallback is not None:
        final_df = final_df.join(patronage_fallback, "participant_id", "left")
        final_df = (
            final_df.withColumn("ICN", coalesce(col("ICN"), col("patronage_icn")))
            .withColumn("edipi", coalesce(col("edipi"), col("patronage_edipi")))
            .drop("patronage_icn", "patronage_edipi")
        )
        if config.LOGGING_VERBOSE:
            log_message(
                "Fallback identity (Patronage ICN/EDIPI) applied for edge case scenarios where correlation is missing.",
                level="DEBUG",
                depth=3,
            )

    final_df = final_df.filter(col("ICN").isNotNull()).withColumn("Batch_CD", lit(SOURCE_TYPE_SCD))

    deduped_source.unpersist()
    target_active.unpersist()

    if config.LOGGING_VERBOSE:
        log_message(f"Found {final_df.count():,} SCD records (Direct + PT-Driven) to process.", level="DEBUG", depth=3)

    return final_df


def _standardize_columns_for_merge(transformed_df: DataFrame, source_type: str) -> DataFrame:
    """Normalize transformed dataframes into a common schema for merge.

    Args:
        transformed_df: Output of `transform_scd_data` or `transform_caregiver_data`.
        source_type: `SOURCE_TYPE_SCD` or `SOURCE_TYPE_CG`.

    Returns:
        DataFrame containing all target columns, filling non-applicable fields
        with NULLs.
    """
    if source_type == SOURCE_TYPE_SCD:
        return transformed_df.select(
            "edipi",
            "ICN",
            "participant_id",
            "SC_Combined_Disability_Percentage",
            "PT_Indicator",
            "Status_Begin_Date",
            "Status_Last_Update",
            "filename",
            "SDP_Event_Created_Timestamp",
            "target_SC_Combined_Disability_Percentage",
            "target_PT_Indicator",
            "target_RecordStatus",
            lit(None).cast(StringType()).alias("Veteran_ICN"),
            lit(None).cast(StringType()).alias("Applicant_Type"),
            lit(None).cast(StringType()).alias("Caregiver_Status"),
            lit(None).cast(StringType()).alias("Individual_Unemployability"),
            lit(None).cast(StringType()).alias("Status_Termination_Date"),
            lit(SOURCE_TYPE_SCD).alias("Batch_CD"),
        )

    return transformed_df.select(
        "edipi",
        "ICN",
        "participant_id",
        "Veteran_ICN",
        "Applicant_Type",
        "Caregiver_Status",
        "Status_Begin_Date",
        "Status_Termination_Date",
        "filename",
        "SDP_Event_Created_Timestamp",
        lit(None).cast(StringType()).alias("SC_Combined_Disability_Percentage"),
        lit(None).cast(StringType()).alias("PT_Indicator"),
        lit(None).cast(StringType()).alias("Individual_Unemployability"),
        lit(None).cast(StringType()).alias("Status_Last_Update"),
        lit(SOURCE_TYPE_CG).alias("Batch_CD"),
    )


def create_and_transform_source_dataframe(
    source_type: str,
    file_list: List[Tuple[str, str]],
    processing_mode: str,
    use_seed_data: bool = False,
    pt_data: Optional[DataFrame] = None,
) -> Tuple[Optional[DataFrame], int]:
    """Read raw source files and return a standardized DataFrame for merge.

    Args:
        source_type: Source identifier (SCD or CG).
        file_list: List of (path, modification_time_iso) from discovery.
        processing_mode: "rebuild" or "update".
        use_seed_data: If True, reads the configured seed file (CG only).
        pt_data: Optional preloaded PT data; when None and source_type is SCD,
            PT data is loaded via `get_pt_data_for_mode`.

    Returns:
        Tuple of (Standardized DataFrame ready for SCD2 merge, or None if no rows,
        raw_row_count for the source read).

    Notes:
                - For SCD with no inbound files, we return None (or an empty skeleton
                    depending on code path) to keep the orchestrator logic simple.
        - Adds `SCDType2_OPERATION_KEY` used by downstream merge logic.
    """
    transformed_df: Optional[DataFrame] = None
    raw_row_count = 0

    if use_seed_data:
        # Seed is CG-only; maintained as a supported code path.
        raw_df = (
            spark.read.csv(PIPELINE_CONFIG[source_type]["seed_file"], header=True, inferSchema=True)
            .selectExpr("*", "_metadata.file_path as filename")
            .withColumn(
                "SDP_Event_Created_Timestamp",
                to_timestamp(lit(PIPELINE_CONFIG[source_type]["beginning_date"]), SPARK_DATETIME_FORMAT),
            )
        )
        raw_row_count = raw_df.count()
        transformed_df = transform_caregiver_data(raw_df)
    else:
        if not file_list:
            if source_type != SOURCE_TYPE_SCD:
                return None

        paths = [f[0] for f in file_list] if file_list else []
        schema = PIPELINE_CONFIG[source_type]["schema"]

        if not paths and source_type == SOURCE_TYPE_SCD:
            raw_df = spark.createDataFrame([], schema).withColumn("filename", lit(None)).withColumn(
                "SDP_Event_Created_Timestamp", lit(None)
            )
        else:
            raw_df = (
                spark.read.schema(schema)
                .csv(paths, header=True)
                .selectExpr(
                    "*",
                    "_metadata.file_path as filename",
                    "_metadata.file_modification_time as SDP_Event_Created_Timestamp",
                )
            )

        raw_row_count = raw_df.count()

        if source_type == SOURCE_TYPE_CG:
            transformed_df = transform_caregiver_data(raw_df)
        elif source_type == SOURCE_TYPE_SCD:
            if pt_data is None:
                pt_data = get_pt_data_for_mode(processing_mode)
            transformed_df = transform_scd_data(raw_df, pt_data, processing_mode)

    if transformed_df is None or transformed_df.isEmpty():
        return None, raw_row_count

    final_df = _standardize_columns_for_merge(transformed_df, source_type)
    return final_df.withColumn("SCDType2_OPERATION_KEY", SPARK_CONFIG[source_type]["concat_column"]), raw_row_count


def initialize_caregiver_seed_data() -> DataFrame:
    """Read and transform the CG seed dataset for initial table population.

    Returns:
        A standardized DataFrame suitable for merge into the Patronage target
        table.

    Raises:
        RuntimeError: if identity lookup is not initialized.
    """
    log_message("Initializing Caregiver seed data...", level="DEBUG", depth=1)

    seed_file_path = PIPELINE_CONFIG[SOURCE_TYPE_CG]["seed_file"]

    caregiver_seed_data = (
        spark.read.csv(seed_file_path, header=True, inferSchema=True)
        .selectExpr("*", "_metadata.file_path as filename")
        .withColumn(
            "SDP_Event_Created_Timestamp",
            to_timestamp(lit(PIPELINE_CONFIG[SOURCE_TYPE_CG]["beginning_date"]), "yyyy-MM-dd HH:mm:ss"),
        )
    )

    transformed_seed_data = (
        caregiver_seed_data.filter(col("ICN").isNotNull() & col("Veteran_ICN").isNotNull() & col("Applicant_Type").isNotNull())
        .dropDuplicates(["ICN", "Veteran_ICN", "Applicant_Type"])
        .select(
            substring("ICN", 1, 10).alias("ICN"),
            "Applicant_Type",
            "Caregiver_Status",
            date_format(to_date(col("Status_Begin_Date"), SPARK_DATE_INPUT_FORMAT), SPARK_DATE_FORMAT).alias("Status_Begin_Date"),
            date_format(
                to_date(col("Status_Termination_Date"), SPARK_DATE_INPUT_FORMAT), SPARK_DATE_FORMAT
            ).alias("Status_Termination_Date"),
            substring("Veteran_ICN", 1, 10).alias("Veteran_ICN"),
            "filename",
            "SDP_Event_Created_Timestamp",
        )
    )

    if state.identity_lookup_table is None:
        raise RuntimeError("Identity lookup table is not initialized. Call initialize_identity_lookup() first.")

    final_seed_df = transformed_seed_data.join(state.identity_lookup_table, "ICN", "left")

    return final_seed_df.select(
        "edipi",
        "ICN",
        "Veteran_ICN",
        "participant_id",
        "Applicant_Type",
        "Caregiver_Status",
        "Status_Begin_Date",
        "Status_Termination_Date",
        "filename",
        "SDP_Event_Created_Timestamp",
        lit(None).cast(StringType()).alias("Status_Last_Update"),
        lit(None).cast(StringType()).alias("SC_Combined_Disability_Percentage"),
        lit(None).cast(StringType()).alias("PT_Indicator"),
        lit(None).cast(StringType()).alias("Individual_Unemployability"),
        lit(SOURCE_TYPE_CG).alias("Batch_CD"),
    )
