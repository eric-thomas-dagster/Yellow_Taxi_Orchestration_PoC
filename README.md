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

## Deploying to Dagster Cloud

The included GitHub Actions workflow (`.github/workflows/dagster-plus-deploy.yml`) is a template. To deploy to your own Dagster Cloud organization:

1. Follow the [Dagster Cloud CI/CD setup guide](https://docs.dagster.io/deployment/dagster-plus/deploying-code/configuring-ci-cd)
2. Run `dg plus deploy configure --git-provider github` to generate a workflow configured for your org
3. Create a CI API token: `dg plus create ci-api-token`
4. Set `DAGSTER_CLOUD_API_TOKEN` as a GitHub secret in your repository

Pushing to `main` will auto-deploy. Pull requests create ephemeral branch deployments for preview.

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
