"""patronage_modularized.identity

Identity correlation build and lookup loading.

This module produces the lookup table used to attach identity fields (e.g.
EDIPI) to Patronage records.

High-level concept:
- MVI data sources provide relationships between:
    - A person ICN (`MVIPersonICN`)
    - Treating facility institution
    - Treating facility person identifier (e.g., EDIPI depending on institution)

We build a wide "correlation lookup" table keyed by ICN with one column per
institution identifier, then rename those columns via `INSTITUTION_MAPPING`.

Duplicate handling:
- Ambiguous correlations are quarantined by identifying duplicates on
    (ICN, institution) and (institution, identifier), removing those from the main
    lookup output, and writing them to a separate duplicates table.

Databricks assumptions:
- Requires `spark` and `dbutils`.
- Writes managed Delta tables with Liquid Clustering at configured locations.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

from databricks.sdk.runtime import *  # noqa: F403
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, first, lit, max, rank  # noqa: F401

from . import state
from .config import (
    CORRELATION_CONFIG,
    DUP_IDENTITY_TABLE_NAME,
    DUP_IDENTITY_TABLE_PATH,
    IDENTITY_TABLE_CLUSTER_COLUMNS,
    IDENTITY_TABLE_NAME,
    IDENTITY_TABLE_PATH,
    INSTITUTION_MAPPING,
    log_message,
    optimize_delta_table,
)


def _filter_psa_data(psa_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Filter the PSA (person-institution association) dataset.

    Args:
        psa_df: Raw PSA DataFrame.
        config: Sub-config containing `filter_condition`, `window_spec`, and
            `columns`.

    Returns:
        Filtered DataFrame with the most recent row per configured partition.
    """
    return (
        psa_df.filter(config["filter_condition"])
        .withColumn("rnk", rank().over(config["window_spec"]))
        .filter(col("rnk") == 1)
        .select(config["columns"])
    )


def _filter_person_data(person_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Filter the person dimension dataset used for ICN status enrichment."""
    return (
        person_df.withColumn("rnk", rank().over(config["window_spec"]))
        .filter(config["filter_condition"])
        .select(config["columns"])
    )


def _filter_institution_data(institution_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Filter the institution dataset to only relevant institution codes."""
    return institution_df.filter(config["filter_condition"]).select(config["columns"])


def _join_psa_with_institutions(psa_df: DataFrame, institution_df: DataFrame) -> DataFrame:
    """Join PSA records to institution metadata and project correlation fields.

    Returns:
        Distinct rows containing ICN, institution code, identifier, and last
        modified timestamp.
    """
    return (
        psa_df.join(
            broadcast(institution_df),
            psa_df["MVITreatingFacilityInstitutionSID"] == institution_df["MVIInstitutionSID"],
            "left",
        )
        .select(
            psa_df["MVIPersonICN"],
            institution_df["InstitutionCode"],
            psa_df["TreatingFacilityPersonIdentifier"],
            psa_df["CorrelationModifiedDateTime"],
        )
        .distinct()
    )


def _identify_duplicate_relationships(joined_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Find duplicate correlations that should be quarantined.

    Duplicate definitions:
    - Same (ICN, InstitutionCode) maps to multiple identifiers.
    - Same (InstitutionCode, Identifier) maps to multiple ICNs.

    Returns:
        A tuple of (person_institution_dups, institution_identifier_dups).
    """
    person_institution_dups = (
        joined_df.groupBy("MVIPersonICN", "InstitutionCode").count().filter(col("count") > 1).cache()
    )

    institution_identifier_dups = (
        joined_df.groupBy("InstitutionCode", "TreatingFacilityPersonIdentifier")
        .count()
        .filter(col("count") > 1)
        .cache()
    )

    return person_institution_dups, institution_identifier_dups


def _remove_duplicate_correlations(
    joined_df: DataFrame, person_institution_dups: DataFrame, institution_identifier_dups: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """Split correlations into clean vs. duplicate (quarantined) sets."""
    correlations_with_flags = joined_df.join(
        broadcast(person_institution_dups.withColumnRenamed("count", "count_iens")),
        ["MVIPersonICN", "InstitutionCode"],
        "left",
    ).join(
        broadcast(institution_identifier_dups.withColumnRenamed("count", "count_icns")),
        ["InstitutionCode", "TreatingFacilityPersonIdentifier"],
        "left",
    )

    clean_correlations = correlations_with_flags.filter(
        col("count_iens").isNull() & col("count_icns").isNull()
    ).drop("count_iens", "count_icns")

    duplicate_correlations = correlations_with_flags.filter(
        col("count_iens").isNotNull() | col("count_icns").isNotNull()
    )

    return clean_correlations, duplicate_correlations


def _calculate_latest_correlation_dates(joined_df: DataFrame) -> DataFrame:
    """Compute the latest correlation modification date per ICN."""
    return joined_df.groupBy("MVIPersonICN").agg(max("CorrelationModifiedDateTime").alias("Last_Modified_Date"))


def _build_correlation_lookup_table(latest_dates_df: DataFrame, clean_correlations_df: DataFrame, person_df: DataFrame) -> DataFrame:
    """Build the wide lookup table keyed by ICN.

    Args:
        latest_dates_df: Per-ICN last modified timestamp.
        clean_correlations_df: Non-duplicate correlation rows.
        person_df: Person dimension slice containing ICN status fields.

    Returns:
        A DataFrame with one row per ICN and one column per configured
        institution identifier, with final columns normalized to include any
        missing mapped columns as NULL.
    """
    pivoted_df = (
        latest_dates_df.join(clean_correlations_df, ["MVIPersonICN"], "left")
        .join(person_df, ["MVIPersonICN"], "left")
        .groupBy("MVIPersonICN", "ICNStatus", "Last_Modified_Date")
        .pivot("InstitutionCode", [code for code in INSTITUTION_MAPPING.keys()])
        .agg(first("TreatingFacilityPersonIdentifier"))
    )

    renamed_df = pivoted_df
    for code, mapping in INSTITUTION_MAPPING.items():
        renamed_df = renamed_df.withColumnRenamed(code, mapping["name"])

    final_column_names = ["MVIPersonICN"] + [m["name"] for m in INSTITUTION_MAPPING.values()] + [
        "ICNStatus",
        "Last_Modified_Date",
    ]

    select_exprs = []
    for col_name in final_column_names:
        if col_name in renamed_df.columns:
            select_exprs.append(col(col_name))
        else:
            select_exprs.append(lit(None).cast("string").alias(col_name))

    return renamed_df.select(*select_exprs)


def build_identity_correlation_table(config: Dict[str, Any] = CORRELATION_CONFIG) -> None:
    """Build/rebuild the identity correlation and duplicate quarantine tables.

    Args:
        config: Correlation configuration including source paths, filter
            conditions, and window specs.

    Side effects:
        - Overwrites the identity lookup Delta table at `IDENTITY_TABLE_PATH`.
        - Overwrites the duplicate correlations Delta table at
          `DUP_IDENTITY_TABLE_PATH`.
        - Runs `OPTIMIZE` on the identity lookup table.
    """
    log_message("Building Identity Correlation Table...")
    identity_start_time = time.time()

    log_message("Loading MVI data sources...", level="DEBUG", depth=1)
    filtered_psa = _filter_psa_data(spark.read.parquet(config["psa"]["path"]), config["psa"])  # noqa: F405
    filtered_person = _filter_person_data(spark.read.parquet(config["person"]["path"]), config["person"])  # noqa: F405
    filtered_institution = _filter_institution_data(
        spark.read.parquet(config["institution"]["path"]), config["institution"]  # noqa: F405
    )

    log_message("Processing joins and duplicates...", level="DEBUG", depth=1)
    joined_data = _join_psa_with_institutions(filtered_psa, filtered_institution)

    log_message("Caching joined data to avoid recomputation...", level="DEBUG", depth=2)
    joined_data.cache()
    joined_data.count()

    person_institution_dups, institution_identifier_dups = _identify_duplicate_relationships(joined_data)
    clean_correlations, duplicate_correlations = _remove_duplicate_correlations(
        joined_data, person_institution_dups, institution_identifier_dups
    )

    log_message("Building final lookup table...", level="DEBUG", depth=1)
    latest_correlation_dates = _calculate_latest_correlation_dates(joined_data)
    lookup_table = _build_correlation_lookup_table(latest_correlation_dates, clean_correlations, filtered_person)

    lookup_table.cache()
    duplicate_correlations.cache()

    log_message("Saving tables with Liquid Clustering...", depth=1)
    dbutils.fs.rm(IDENTITY_TABLE_PATH, True)  # noqa: F405
    (
        lookup_table.write.option("path", IDENTITY_TABLE_PATH)
        .option("overwriteSchema", "true")
        .option("clusterBy", ",".join(IDENTITY_TABLE_CLUSTER_COLUMNS))
        .mode("overwrite")
        .saveAsTable(IDENTITY_TABLE_NAME)
    )

    optimize_delta_table(IDENTITY_TABLE_NAME)

    dbutils.fs.rm(DUP_IDENTITY_TABLE_PATH, True)  # noqa: F405
    duplicate_correlations.write.option("path", DUP_IDENTITY_TABLE_PATH).mode("overwrite").saveAsTable(
        DUP_IDENTITY_TABLE_NAME
    )

    log_message("Cleaning up cached DataFrames...", level="DEBUG", depth=1)
    lookup_table.unpersist()
    duplicate_correlations.unpersist()
    joined_data.unpersist()
    person_institution_dups.unpersist()
    institution_identifier_dups.unpersist()

    identity_end_time = time.time()
    log_message(f"Identity Correlation Table built in {(identity_end_time - identity_start_time) / 60:.2f} minutes.")


def _check_daily_identity_rebuild_needed() -> bool:
    """Determine whether the identity lookup should be rebuilt today.

    Returns:
        True if the Delta table's lastModified date is earlier than today, or if
        we cannot determine status; otherwise False.

    Notes:
        The pipeline expects the identity correlation to be refreshed daily.
    """
    try:
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{IDENTITY_TABLE_PATH}`").collect()  # noqa: F405
        if detail:
            last_modified = detail[0]["lastModified"]
            last_modified_date = last_modified.date() if hasattr(last_modified, "date") else last_modified
            today = datetime.now(timezone.utc).date()
            if last_modified_date < today:
                log_message(f"Identity table last modified: {last_modified_date}, rebuilding for today ({today})")
                return True
            log_message(f"Identity table already built today ({today})")
            return False
    except Exception as e:
        log_message(f"Could not check identity table status: {str(e)}, will rebuild.", level="INFO")
        return True

    return True


def initialize_identity_lookup() -> DataFrame:
    """Load the identity lookup table into shared state (rebuilding if needed).

    Returns:
        The cached identity lookup DataFrame.

    Side effects:
        Populates `state.identity_lookup_table` so other modules can join to it.
    """
    if state.identity_lookup_table is None:
        if _check_daily_identity_rebuild_needed():
            build_identity_correlation_table()

        log_message("Loading identity lookup table...")

        identity_columns_to_select = ["ICN"] + [
            mapping["name"] for mapping in INSTITUTION_MAPPING.values() if mapping["name"] != "va_profile_id"
        ]

        state.identity_lookup_table = (
            spark.read.format("delta")  # noqa: F405
            .load(IDENTITY_TABLE_PATH)
            .withColumnRenamed("MVIPersonICN", "ICN")
            .select(*identity_columns_to_select)
        )

        # Optional verbose count for operational visibility.
        from . import config

        if config.LOGGING_VERBOSE:
            log_message(f"Loaded identity table with {state.identity_lookup_table.count():,} records", depth=1)

    return state.identity_lookup_table
