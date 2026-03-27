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

### Azure DevOps

An Azure DevOps pipeline is included at `azure-pipelines.yml`. To set it up:

1. Generate a CI API token (run locally, one-time):
   ```bash
   pip install dagster-cloud
   dg plus create ci-api-token --description "Azure DevOps pipeline"
   ```

2. In Azure DevOps, create a **Variable Group** called `dagster-cloud` with:
   - `DAGSTER_CLOUD_API_TOKEN` (mark as secret): the token from step 1
   - `DAGSTER_CLOUD_ORGANIZATION`: your Dagster+ organization name

3. Create a pipeline pointing at `azure-pipelines.yml`

Pushing to `main` deploys to production. Pull requests create ephemeral branch deployments for preview.

### GitHub Actions

A GitHub Actions workflow is also included at `.github/workflows/dagster-plus-deploy.yml`. To use it instead:

1. Follow the [Dagster Cloud CI/CD setup guide](https://docs.dagster.io/deployment/dagster-plus/deploying-code/configuring-ci-cd)
2. Run `dg plus deploy configure --git-provider github` to regenerate the workflow for your org
3. Set `DAGSTER_CLOUD_API_TOKEN` as a GitHub secret

### Other CI Systems

Dagster Cloud supports deployment from any CI system via the CLI:

```bash
pip install dagster-cloud
export DAGSTER_CLOUD_API_TOKEN="your-token"
export DAGSTER_CLOUD_ORGANIZATION="your-org"

dagster-cloud serverless deploy-python-executable . \
  --location-name orchestration-poc \
  --package-name orchestration_poc \
  --python-version 3.12
```

See the [Dagster Cloud CI/CD docs](https://docs.dagster.io/deployment/dagster-plus/deploying-code/configuring-ci-cd) for more details.

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
