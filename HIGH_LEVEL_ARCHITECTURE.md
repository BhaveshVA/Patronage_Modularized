# Patronage ETL Pipeline

A Databricks Patronage data pipeline for ETL processing using Apache Spark and Delta Lake.

---

This document is intentionally architecture-focused; operational runbooks and validation steps live in separate docs.
## High level Architecture
```mermaid
flowchart LR
    subgraph Sources
        direction LR
        CaregiverCSVs["Caregiver CSVs"]
        SCDCSVs["SCD CSVs"]
        IdentityCorrelations["Identity Correlations Delta"]
        NewPTDelta["New PT Delta"]
    end
    subgraph Processing
        direction LR
        SeedLoader["Seed Loader"]
        CaregiverAggregator["Caregiver Aggregator"]
        SCDPreparer["SCD Preparer"]
        PTIndicatorUpdater["PT Indicator Updater"]
        AuditLog["Audit Log"]
    end
    SCDType["Slowly Changing Dimensions Type 2"]
    subgraph Storage
        DeltaTable["Delta Table: Patronage"]
    end
    
    CaregiverCSVs --> SeedLoader
    CaregiverCSVs --> CaregiverAggregator
    SeedLoader --> CaregiverAggregator
    SCDCSVs --> SCDPreparer
    IdentityCorrelations --> SCDPreparer
    NewPTDelta --> PTIndicatorUpdater
    IdentityCorrelations --> SeedLoader
    
    SeedLoader --> SCDType
    SCDPreparer --> SCDType
    PTIndicatorUpdater --> SCDType
    CaregiverAggregator --> SCDType
    AuditLog --> SCDType
    SCDType --> DeltaTable
    
    SCDCSVs --> SeedLoader
    IdentityCorrelations --> CaregiverAggregator
    NewPTDelta --> AuditLog
    
    SeedLoader -.-> AuditLog
    SCDPreparer -.-> AuditLog
    PTIndicatorUpdater -.-> AuditLog
    CaregiverAggregator -.-> AuditLog
    linkStyle 0 stroke:#1f77b4,stroke-width:2px;
    linkStyle 1 stroke:#1f77b4,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 2 stroke:#1f77b4,stroke-width:2px,stroke-dasharray: 2 4;
    linkStyle 3 stroke:#ff7f0e,stroke-width:2px;
    linkStyle 4 stroke:#2ca02c,stroke-width:2px;
    linkStyle 5 stroke:#d62728,stroke-width:2px;
    linkStyle 6 stroke:#2ca02c,stroke-width:2px,stroke-dasharray: 2 4;
    linkStyle 7 stroke:#9467bd,stroke-width:2px;
    linkStyle 8 stroke:#9467bd,stroke-width:2px;
    linkStyle 9 stroke:#9467bd,stroke-width:2px;
    linkStyle 10 stroke:#9467bd,stroke-width:2px;
    linkStyle 11 stroke:#9467bd,stroke-width:2px;
    linkStyle 12 stroke:#8c564b,stroke-width:2px;
    linkStyle 13 stroke:#ff7f0e,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 14 stroke:#2ca02c,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 15 stroke:#d62728,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 16 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
    linkStyle 17 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
    linkStyle 18 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
    linkStyle 19 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
```
---

## ETL Workflow Diagram

```mermaid
flowchart LR
    A[Raw Data Sources] --> B[File Discovery]
    B --> C[Data Preparation]
    C -- Deduplication --> D["Delta Manager SCD2 Upsert"]
    D -- Change Tracking --> E[Delta Table]
    E --> F[Reporting & Analytics]
```

High-level ETL workflow showing the flow from raw sources through processing steps to the Delta table and reporting.

---

## Data Lineage Diagram

```mermaid
flowchart LR
    subgraph Sources
        A1[Caregiver CSV]
        A2[SCD CSV]
        A3[PT Indicator Delta Table]
        A4[Identity Correlations Delta Table]
    end
    A1 --> B[Data Preparation]
    A2 --> B
    A3 --> B
    A4 --> B
    B --> C[SCD2 Upsert Logic]
    C --> D[Delta Table]
    D --> E[Reporting]
```

Data lineage diagram showing how data moves from all sources, through each transformation, and into the Delta table for reporting.

---

## SCD2 Upsert Logic Flow

```mermaid
flowchart LR
    A[Incoming Record] --> B{Record Exists in Delta Table?}
    B -- No --> C[Insert as New Record]
    B -- Yes --> D{Data Changed?}
    D -- No --> E[No Action]
    D -- Yes --> F[Expire Old Record]
    F --> G[Insert as New Version]
    G --> H[Update Delta Table]
```

SCD2 upsert logic flow showing how new, changed, and unchanged records are handled in the Delta table.

---

## Business Logic Overview

- **Inputs:** Inbound SCD and Caregiver CSVs plus identity-correlation sources and PT indicator Delta sources.
- **Core flow:** Discover inbound CSVs, transform/dedupe/enrich, then SCD2 merge into the Patronage Delta table.
- **SCD2 semantics:** On change, expire the previous active row and insert a new active version; unchanged rows are not rewritten.
- **Operational tasks:** After core processing, scheduled DMDC export and monthly EDIPI backfill may run when gated.

## References
- Delta Lake docs: https://docs.delta.io/latest/delta-intro.html
- PySpark docs: https://spark.apache.org/docs/latest/api/python/

---
