# NYC Yellow Taxi - Dagster Orchestration PoC

Dagster project orchestrating the NYC Yellow Taxi data pipeline across Databricks and Snowflake with monthly partitions for temporal data processing windows.

## Quick Start

```bash
uv sync
uv run dagster dev
```

Open http://localhost:3000 in your browser to see the project.

## Architecture

```
Databricks Ingestion       dbt Snowflake (monthly partitioned)              Databricks Export
───────────────────       ──────────────────────────────────────            ──────────────────
source_taxi_zone_lookup → stg_taxi_zone_lookup → dim_location ─┐
                                                                ├→ fact_yellow_taxi_trips
source_yellow_tripdata  → stg_yellow_taxi_trips ───────────────┘         │
                                                       ┌─────────────────┤
                                                       ↓                 ↓
                                                agg_daily_zone    agg_hourly_demand
                                                agg_monthly_zone  agg_vendor_performance
                                                       │
                                                       ↓
                                                export_aggregate_data
```

## Going Live

Set `demo_mode: false` in the Databricks YAML configs and configure:

- `DATABRICKS_TOKEN` env var + `cluster_id` in YAML
- `target: snowflake` in dbt YAML + Snowflake env vars (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`)
- Update `source_database` var to `WBMIQA_ORCHPOC_DB`

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
