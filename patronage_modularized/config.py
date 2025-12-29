from __future__ import annotations

"""Central configuration for the Patronage pipeline.

Why this file exists
--------------------
This module is the single source of truth for:
- Table names and storage locations (Delta tables, export directories)
- Input schemas and file discovery rules for each source (SCD, CG)
- Spark window specifications and merge conditions used by SCD Type 2 logic
- DMDC eligibility filters and fixed-width record formatting

Maintenance rules
-----------------
1) Prefer changing values here rather than scattering constants across modules.
2) Treat `PIPELINE_CONFIG` and `SPARK_CONFIG` as "contracts" with other modules:
    `discovery.py`, `transforms.py`, and `scd2.py` read these keys.
3) Keep business logic (e.g., eligibility rules) in one place to avoid drift.

Runtime assumptions
-------------------
This package is designed to run inside Databricks where `spark` and `dbutils`
are available (via the Databricks runtime).
"""

import inspect
from datetime import datetime
from typing import Any, Dict

# Databricks injects runtime globals like `spark` and `dbutils`.
# We import the runtime shim so these names are available when running in
# Databricks, while still allowing local editing.
from databricks.sdk.runtime import *
from pyspark.sql.functions import (
    asc,
    broadcast,
    coalesce,
    col,
    concat,
    concat_ws,
    date_format,
    desc,
    first,
    lit,
    lpad,
    max,
    rank,
    row_number,
    substring,
    to_date,
    to_timestamp,
    trim,
    when,
    xxhash64,
)
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

# =====================================================================================
# LOGGING CONFIGURATION
# =====================================================================================
LOGGING_VERBOSE = False

# =====================================================================================
# SOURCE TYPE CONSTANTS
# =====================================================================================
SOURCE_TYPE_SCD = "SCD"
SOURCE_TYPE_CG = "CG"


def log_message(message: str, level: str = "INFO", depth: int = 0) -> None:
    """Print a structured, readable log line.

    This helper provides consistent formatting across modules and supports a
    simple verbosity toggle via `LOGGING_VERBOSE`.

    Args:
        message: Human-readable message.
        level: One of "INFO", "DEBUG", "WARN", "ERROR".
        depth: Indentation level (each level adds two leading spaces).
    """
    if level in ("INFO", "WARN", "ERROR") or (level == "DEBUG" and LOGGING_VERBOSE):
        timestamp = datetime.now().strftime(PY_DATETIME_FORMAT)
        caller = inspect.stack()[1].function
        indent = "  " * depth
        caller_str = "" if caller == "<module>" else caller
        if caller_str:
            print(f"[{level:5}] | {timestamp} | {caller_str:40} | {indent}{message}")
        else:
            print(f"[{level:5}] | {timestamp} | {indent}{message}")


def optimize_delta_table(table_name: str) -> None:
    """Run Databricks `OPTIMIZE` against a Delta table.

    Notes:
        This pipeline uses Liquid Clustering (`CLUSTER BY`) and therefore
        intentionally does not use `ZORDER BY`.

    Args:
        table_name: Spark SQL table name.
    """
    spark.sql(f"OPTIMIZE {table_name}")
    log_message(f"Optimized table {table_name} (Liquid Clustering enabled)")


# =====================================================================================
# DATE FORMAT CONSTANTS
# =====================================================================================
PY_DATE_FORMAT = "%Y-%m-%d"
PY_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PY_DATE_COMPACT_FORMAT = "%Y%m%d"

SPARK_DATE_FORMAT = "yyyyMMdd"
SPARK_DATE_INPUT_FORMAT = "yyyy-MM-dd"
SPARK_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

# =====================================================================================
# CORRELATION CONFIGURATION
# =====================================================================================

INSTITUTION_MAPPING = {
    "200CORP": {"sid": 5667, "name": "participant_id"},
    "200DOD": {"sid": 6061, "name": "edipi"},
    "200VETS": {"sid": 6722, "name": "va_profile_id"},
}

CORRELATION_CONFIG: Dict[str, Any] = {
    "psa": {
        "path": "/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/",
        "columns": [
            "MVIPersonICN",
            "MVITreatingFacilityInstitutionSID",
            "TreatingFacilityPersonIdentifier",
            "ActiveMergedIdentifier",
            "CorrelationModifiedDateTime",
        ],
        "filter_condition": (
            (col("MVIPersonICN").isNotNull())
            & (col("MVITreatingFacilityInstitutionSID").isin([v["sid"] for v in INSTITUTION_MAPPING.values()]))
            & ((col("ActiveMergedIdentifier") == "Active") | col("ActiveMergedIdentifier").isNull())
        ),
        "window_spec": Window.partitionBy(
            "MVIPersonICN", "MVITreatingFacilityInstitutionSID", "TreatingFacilityPersonIdentifier"
        ).orderBy(desc("CorrelationModifiedDateTime")),
    },
    "person": {
        "path": "/mnt/ci-mvi/Processed/SVeteran.SMVIPerson/",
        "columns": ["MVIPersonICN", "ICNStatus"],
        "filter_condition": (col("rnk") == 1),
        "window_spec": Window.partitionBy("MVIPersonICN").orderBy(desc("calc_IngestionTimestamp")),
    },
    "institution": {
        "path": "/mnt/ci-mvi/Raw/NDim.MVIInstitution/",
        "columns": ["MVIInstitutionSID", "InstitutionCode"],
        "filter_condition": (col("MVIInstitutionSID").isin([v["sid"] for v in INSTITUTION_MAPPING.values()])),
        "window_spec": None,
    },
}

# =====================================================================================
# MAIN PIPELINE CONFIGURATION
# =====================================================================================

PATRONAGE_TABLE_SCHEMA = """
    edipi STRING, ICN STRING, Veteran_ICN STRING, participant_id STRING, Batch_CD STRING,
    Applicant_Type STRING, Caregiver_Status STRING, SC_Combined_Disability_Percentage STRING,
    PT_Indicator STRING, Individual_Unemployability STRING, Status_Begin_Date STRING,
    Status_Last_Update STRING, Status_Termination_Date STRING, SDP_Event_Created_Timestamp TIMESTAMP,
    filename STRING, RecordLastUpdated DATE, RecordStatus BOOLEAN, sentToDoD BOOLEAN,
    change_log STRING, RecordChangeStatus STRING
"""

PATRONAGE_TABLE_CLUSTER_COLUMNS = "ICN"
IDENTITY_TABLE_CLUSTER_COLUMNS = ["MVIPersonICN", "participant_id"]


def build_dmdc_record_sql(edipi_column: str = "edipi") -> str:
    """Return Spark SQL that builds a DMDC fixed-width record.

    DMDC format (42 chars total):
    - EDIPI (10)
    - Batch_CD (3)
    - SC disability % (3)
    - Status begin date (8, yyyyMMdd)
    - PT indicator (1)
    - Individual unemployability (1)
    - Status last update (8, yyyyMMdd)
    - Status termination date (8, yyyyMMdd)

    The output is a single concatenated string expression suitable for
    `SELECT <expr> AS DMDC_Record ...`.

    Args:
        edipi_column: Column name/expression used for the EDIPI segment.

    Returns:
        A Spark SQL expression string.
    """
    return f"""rpad(coalesce({edipi_column},''),10,' ') ||
        rpad(coalesce(Batch_CD,''),3,' ') ||
        rpad(coalesce(SC_Combined_Disability_Percentage,''),3,' ') ||
        rpad(coalesce(Status_Begin_Date,''),8,' ') ||
        rpad(coalesce(PT_Indicator,''),1,' ') ||
        rpad(coalesce(Individual_Unemployability,''),1,' ') ||
        rpad(coalesce(Status_Last_Update,''),8,' ') ||
        rpad(coalesce(Status_Termination_Date,''),8,' ')"""


DMDC_CG_ELIGIBILITY_FILTER = f"""
    Batch_CD = '{SOURCE_TYPE_CG}'
    AND Applicant_Type = 'Primary Caregiver'
    AND (
        Status_Termination_Date IS NULL
        OR Status_Termination_Date >= date_format(current_date(), '{SPARK_DATE_FORMAT}')
        OR Caregiver_Status IN ('Approved', 'Pending Revocation/Discharge')
    )
"""

DMDC_ELIGIBILITY_FILTER = f"""
    (Batch_CD = '{SOURCE_TYPE_SCD}')
    OR
    ({DMDC_CG_ELIGIBILITY_FILTER})
"""

PIPELINE_CONFIG: Dict[str, Any] = {
    SOURCE_TYPE_CG: {
        "seed_file": "dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv",
        "path": "/mnt/ci-carma/landing/",
        "beginning_date": "2024-12-18 20:00:00",
        "file_extension": "csv",
        "matching_characters": "caregiver",
        "input_date_format": SPARK_DATE_INPUT_FORMAT,
        "output_date_format": SPARK_DATE_FORMAT,
        "schema": StructType(
            [
                StructField("Discharge_Revocation_Date__c", StringType(), True),
                StructField("Caregiver_Status__c", StringType(), True),
                StructField("CreatedById", StringType(), True),
                StructField("Dispositioned_Date__c", StringType(), True),
                StructField("CARMA_Case_ID__c", StringType(), True),
                StructField("Applicant_Type__c", StringType(), True),
                StructField("CreatedDate", TimestampType(), True),
                StructField("Veteran_ICN__c", StringType(), True),
                StructField("Benefits_End_Date__c", StringType(), True),
                StructField("Caregiver_Id__c", StringType(), True),
                StructField("CARMA_Case_Number__c", StringType(), True),
                StructField("Caregiver_ICN__c", StringType(), True),
            ]
        ),
    },
    SOURCE_TYPE_SCD: {
        "path": "/mnt/ci-vadir-shared/",
        "beginning_date": "2024-06-30",
        "file_extension": "csv",
        "matching_characters": "CPIDODIEX_",
        "input_date_format": "MMddyyyy",
        "output_date_format": SPARK_DATE_FORMAT,
        "schema": StructType(
            [
                StructField("PTCPNT_ID", StringType()),
                StructField("CMBNED_DEGREE_DSBLTY", StringType()),
                StructField("DSBL_DTR_DT", StringType()),
            ]
        ),
        "pt_sources": {
            "seed_file": "dbfs:/user/hive/warehouse/pai",
            "delta_table": "/mnt/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/",
        },
    },
}

SPARK_CONFIG: Dict[str, Any] = {
    SOURCE_TYPE_CG: {
        "window_spec": Window.partitionBy("Caregiver_ICN__c", "Veteran_ICN__c", "Applicant_Type__c").orderBy(
            desc(col("CreatedDate"))
        ),
        "delta_conditions": xxhash64(
            col("Status_Begin_Date"),
            col("Status_Termination_Date"),
            col("Applicant_Type"),
            col("Caregiver_Status"),
        )
        != xxhash64(
            col("target_Status_Begin_Date"),
            col("target_Status_Termination_Date"),
            col("target_Applicant_Type"),
            col("target_Caregiver_Status"),
        ),
        "target_join_keys": ["ICN", "Veteran_ICN", "Applicant_Type", "Batch_CD"],
        "join_conditions": (
            (col("ICN") == col("target_ICN"))
            & (col("Veteran_ICN") == col("target_Veteran_ICN"))
            & (col("Batch_CD") == col("target_Batch_CD"))
            & (col("Applicant_Type") == col("target_Applicant_Type"))
            & (col("target_RecordStatus") == True)
        ),
        "merge_conditions": "((concat(target.ICN, target.Veteran_ICN, target.Applicant_Type) = source.SCDType2_OPERATION_KEY) and (target.RecordStatus = True))",
        "expiration_condition": (
            col("target_ICN").isNotNull()
            & col("target_Veteran_ICN").isNotNull()
            & col("target_Applicant_Type").isNotNull()
            & (col("target_RecordStatus") == True)
        ),
        "concat_column": concat(col("ICN"), col("Veteran_ICN"), col("Applicant_Type")),
        "columns_to_track": [
            ("Status_Begin_Date", "target_Status_Begin_Date"),
            ("Status_Termination_Date", "target_Status_Termination_Date"),
            ("Applicant_Type", "target_Applicant_Type"),
            ("Caregiver_Status", "target_Caregiver_Status"),
        ],
    },
    SOURCE_TYPE_SCD: {
        "window_spec": Window.partitionBy("participant_id").orderBy(
            desc("SDP_Event_Created_Timestamp"),
            desc("DSBL_DTR_DT"),
            desc("SC_Combined_Disability_Percentage"),
        ),
        "source_dedup_window_spec": Window.partitionBy("participant_id").orderBy(
            desc("source_DSBL_DTR_DT_parsed"),
            desc("source_SC_Combined_Disability_Percentage"),
        ),
        "delta_conditions": (
            (xxhash64(col("SC_Combined_Disability_Percentage")) != xxhash64(col("target_SC_Combined_Disability_Percentage")))
            | (xxhash64(col("PT_Indicator")) != xxhash64(col("target_PT_Indicator")))
        ),
        "target_join_keys": ["ICN", "Batch_CD"],
        "join_conditions": (
            (col("ICN") == col("target_ICN"))
            & (col("target_RecordStatus") == True)
            & (col("Batch_CD") == col("target_Batch_CD"))
        ),
        "merge_conditions": f"((target.participant_id = source.SCDType2_OPERATION_KEY) and (target.Batch_CD = '{SOURCE_TYPE_SCD}') and (target.RecordStatus = True))",
        "expiration_condition": (col("target_SC_Combined_Disability_Percentage").isNotNull() & (col("target_RecordStatus") == True)),
        "concat_column": col("participant_id"),
        "columns_to_track": [
            ("SC_Combined_Disability_Percentage", "target_SC_Combined_Disability_Percentage"),
            ("PT_Indicator", "target_PT_Indicator"),
        ],
    },
}

TARGET_TABLE_COLUMN_MAPPING: Dict[str, Any] = {
    "edipi": "source.edipi",
    "ICN": "source.ICN",
    "Veteran_ICN": "source.Veteran_ICN",
    "Applicant_Type": "source.Applicant_Type",
    "Caregiver_Status": "source.Caregiver_Status",
    "participant_id": "source.participant_id",
    "Batch_CD": "source.Batch_CD",
    "SC_Combined_Disability_Percentage": "source.SC_Combined_Disability_Percentage",
    "PT_Indicator": "source.PT_Indicator",
    "Individual_Unemployability": "source.Individual_Unemployability",
    "Status_Begin_Date": "source.Status_Begin_Date",
    "Status_Last_Update": "source.Status_Last_Update",
    "Status_Termination_Date": "source.Status_Termination_Date",
    "SDP_Event_Created_Timestamp": "source.SDP_Event_Created_Timestamp",
    "RecordStatus": "true",
    "RecordLastUpdated": "to_date(source.SDP_Event_Created_Timestamp)",
    "filename": "source.filename",
    "sentToDoD": "false",
    "change_log": "source.change_log",
    "RecordChangeStatus": "source.RecordChangeStatus",
}

# =====================================================================================
# GLOBAL CONSTANTS (Paths / Tables)
# =====================================================================================

PATRONAGE_TABLE_NAME = "patronage_unified"
PATRONAGE_TABLE_PATH = f"dbfs:/user/hive/warehouse/{PATRONAGE_TABLE_NAME}"
IDENTITY_TABLE_NAME = "correlation_lookup"
IDENTITY_TABLE_PATH = f"/mnt/ci-patronage/delta_tables/{IDENTITY_TABLE_NAME}"
DUP_IDENTITY_TABLE_NAME = "duplicate_correlations"
DUP_IDENTITY_TABLE_PATH = f"/mnt/ci-patronage/delta_tables/{DUP_IDENTITY_TABLE_NAME}"
DMDC_CHECKPOINT_TABLE_NAME = "dmdc_checkpoint_test"

DMDC_EXPORT_DIR = "dbfs/mnt/ci-patronage/dmdc_extracts/test/combined_export"
