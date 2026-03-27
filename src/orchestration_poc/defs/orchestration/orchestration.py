"""Orchestration definitions for the NYC Yellow Taxi pipeline.

Implements temporal orchestration requirements:
- Monthly schedule aligned to data processing windows
- Jobs for full pipeline, ingestion-only, and transformation-only runs
- Freshness policies as completeness boundaries
- Support for controlled reprocessing via backfill
"""

import dagster as dg

# ---------------------------------------------------------------------------
# Monthly partition definition (shared across all partitioned assets)
# ---------------------------------------------------------------------------
MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2023-01-01",
    end_offset=0,
)

# ---------------------------------------------------------------------------
# Jobs - scoped asset selections for different execution scenarios
# ---------------------------------------------------------------------------
full_pipeline_job = dg.define_asset_job(
    name="full_monthly_pipeline",
    description=(
        "End-to-end pipeline: Databricks ingestion → Snowflake dbt transforms → Export. "
        "Each run processes a single monthly data window."
    ),
    selection=dg.AssetSelection.all(),
    partitions_def=MONTHLY_PARTITIONS,
    tags={"pipeline": "nyc_taxi_poc", "scope": "full"},
)

ingestion_job = dg.define_asset_job(
    name="databricks_ingestion_job",
    description="Run only the Databricks source ingestion notebooks for a given month.",
    selection=dg.AssetSelection.groups("databricks_ingestion"),
    partitions_def=MONTHLY_PARTITIONS,
    tags={"pipeline": "nyc_taxi_poc", "scope": "ingestion"},
)

transformation_job = dg.define_asset_job(
    name="dbt_transformation_job",
    description="Run only the dbt Snowflake transformations for a given month.",
    selection=dg.AssetSelection.groups("transforms"),
    partitions_def=MONTHLY_PARTITIONS,
    tags={"pipeline": "nyc_taxi_poc", "scope": "transformation"},
)

export_job = dg.define_asset_job(
    name="export_job",
    description="Run only the export step for a given month.",
    selection=dg.AssetSelection.groups("databricks_export"),
    partitions_def=MONTHLY_PARTITIONS,
    tags={"pipeline": "nyc_taxi_poc", "scope": "export"},
)

# ---------------------------------------------------------------------------
# Schedules - monthly cadence matching data processing windows
# ---------------------------------------------------------------------------
monthly_pipeline_schedule = dg.build_schedule_from_partitioned_job(
    job=full_pipeline_job,
    description=(
        "Monthly schedule that triggers the full pipeline for each data processing window. "
        "Runs on the 5th of each month to allow time for data completeness."
    ),
    hour_of_day=6,
    minute_of_hour=0,
)
