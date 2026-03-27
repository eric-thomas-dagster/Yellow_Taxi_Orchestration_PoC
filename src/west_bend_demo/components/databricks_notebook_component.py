"""Databricks notebook component for West Bend POC.

Wraps Databricks notebook execution as Dagster assets with monthly partition support.
Supports both demo mode (simulated execution) and live mode (real Databricks SDK calls).

Notebooks in the POC:
  - source_taxi_zone_lookup: one-time reference load (ENVIRONMENT param)
  - source_yellow_tripdata: monthly ingestion (YEAR, MONTH, ENVIRONMENT params)
  - export_aggregate_data: monthly export (YEAR, MONTH, ENVIRONMENT params)

Each notebook receives YEAR/MONTH extracted from the Dagster partition key,
matching West Bend's existing parameterization pattern.
"""

import os
import random
import time
from typing import Optional

import dagster as dg
from dagster_databricks import DatabricksClientResource


class NotebookSpec(dg.Model):
    """Specification for a single Databricks notebook asset."""

    notebook_path: str
    asset_key: str
    group_name: str = "databricks"
    description: Optional[str] = None
    deps: list[str] = []
    kinds: list[str] = ["databricks", "python"]
    partitioned: bool = True
    notebook_params: dict[str, str] = {}


class DatabricksNotebookComponent(dg.Component, dg.Model, dg.Resolvable):
    """A component that creates Dagster assets from Databricks notebook executions.

    Supports monthly partitions for temporal data processing windows.
    Each notebook run receives YEAR and MONTH parameters extracted from the
    partition key, plus any static notebook_params from the YAML config.

    In live mode, uses the Databricks SDK to submit one-time notebook runs
    via the Jobs API (runs/submit) and polls until completion.

    In demo mode, simulates execution with realistic metadata output.
    """

    workspace_host: str = "https://westbend.cloud.databricks.com"
    token: Optional[str] = None
    environment: str = "qa_poc"
    cluster_id: Optional[str] = None
    notebooks: list[NotebookSpec] = []
    demo_mode: bool = True
    poll_interval_seconds: float = 10.0
    max_wait_time_seconds: float = 3600.0

    def _is_demo_mode(self) -> bool:
        return self.demo_mode or os.getenv("DAGSTER_DEMO_MODE", "false").lower() == "true"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        monthly_partitions = dg.MonthlyPartitionsDefinition(
            start_date="2023-01-01",
            end_offset=0,
        )

        assets = []
        for notebook_spec in self.notebooks:
            assets.append(self._make_notebook_asset(notebook_spec, monthly_partitions))

        resources: dict = {}
        if not self._is_demo_mode():
            resources["databricks"] = DatabricksClientResource(
                host=self.workspace_host,
                token=self.token or os.environ.get("DATABRICKS_TOKEN", ""),
            )

        return dg.Definitions(assets=assets, resources=resources)

    def _make_notebook_asset(
        self, spec: NotebookSpec, monthly_partitions: dg.MonthlyPartitionsDefinition
    ) -> dg.AssetsDefinition:
        is_demo = self._is_demo_mode()
        workspace_host = self.workspace_host
        environment = self.environment
        cluster_id = self.cluster_id
        poll_interval = self.poll_interval_seconds
        max_wait = self.max_wait_time_seconds

        # Support slash-separated keys like "mrt/agg_monthly_zone_summary"
        deps = (
            [dg.AssetKey(dep.split("/")) for dep in spec.deps]
            if spec.deps
            else None
        )

        # Capture spec values for the closure
        notebook_path = spec.notebook_path
        static_params = dict(spec.notebook_params)
        asset_key = spec.asset_key
        is_partitioned = spec.partitioned

        @dg.asset(
            name=asset_key,
            group_name=spec.group_name,
            description=spec.description,
            kinds=set(spec.kinds),
            deps=deps,
            partitions_def=monthly_partitions if is_partitioned else None,
            automation_condition=dg.AutomationCondition.eager() if is_partitioned else None,
            tags={"orchestrator": "dagster", "compute_platform": "databricks"},
        )
        def _notebook_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            # Build notebook parameters matching the POC's YEAR/MONTH/ENVIRONMENT pattern
            params = {"ENVIRONMENT": environment, **static_params}

            if is_partitioned:
                partition_key = context.partition_key  # e.g. "2023-06-01"
                year = partition_key[:4]
                month = partition_key[5:7]
                params["YEAR"] = year
                params["MONTH"] = month
                context.log.info(
                    f"Processing data window: {year}-{month} "
                    f"(partition {partition_key})"
                )
            else:
                year = None
                month = None

            if is_demo:
                return _run_demo(context, params, workspace_host, notebook_path, year, month)
            else:
                return _run_live(
                    context, params, notebook_path, cluster_id,
                    poll_interval, max_wait, year, month,
                )

        def _run_demo(
            context: dg.AssetExecutionContext,
            params: dict,
            host: str,
            path: str,
            year: Optional[str],
            month: Optional[str],
        ) -> dg.MaterializeResult:
            context.log.info(f"[DEMO MODE] Notebook: {path}")
            context.log.info(f"[DEMO MODE] Workspace: {host}")
            context.log.info(f"[DEMO MODE] Parameters: {params}")
            time.sleep(random.uniform(0.5, 2.0))

            rows = random.randint(500_000, 2_000_000) if year else 265
            run_id = random.randint(100000, 999999)

            metadata: dict = {
                "databricks/notebook_path": path,
                "databricks/workspace": host,
                "databricks/run_id": run_id,
                "databricks/environment": params["ENVIRONMENT"],
                "rows_processed": rows,
            }
            if year and month:
                metadata["partition/year"] = year
                metadata["partition/month"] = month

            return dg.MaterializeResult(metadata=metadata)

        def _run_live(
            context: dg.AssetExecutionContext,
            params: dict,
            path: str,
            cluster: Optional[str],
            poll_interval: float,
            max_wait: float,
            year: Optional[str],
            month: Optional[str],
        ) -> dg.MaterializeResult:
            """Execute notebook on Databricks via the Jobs runs/submit API."""
            databricks = context.resources.databricks  # type: ignore[attr-defined]
            client = databricks.get_client()
            ws = client.workspace_client

            # Build the submit run payload
            notebook_task = {"notebook_path": path, "base_parameters": params}
            task_config: dict = {
                "task_key": f"dagster_{asset_key}",
                "notebook_task": notebook_task,
            }

            # Use existing cluster or let Databricks pick one
            if cluster:
                task_config["existing_cluster_id"] = cluster
            else:
                # If no cluster_id, user must provide new_cluster config or
                # the notebook must be in a repo with a default cluster.
                # For the POC, we expect cluster_id to be set.
                context.log.warning(
                    "No cluster_id configured. Databricks will use the "
                    "notebook's default cluster if available."
                )

            context.log.info(f"Submitting notebook run: {path}")
            context.log.info(f"Parameters: {params}")

            # Submit the run
            run = ws.jobs.submit(
                run_name=f"dagster-{asset_key}-{year or 'full'}-{month or ''}",
                tasks=[task_config],
            )
            run_id = run.run_id
            context.log.info(f"Submitted Databricks run {run_id}")

            # Poll until completion
            client.wait_for_run_to_complete(
                logger=context.log,
                databricks_run_id=run_id,
                poll_interval_sec=poll_interval,
                max_wait_time_sec=max_wait,
            )

            # Check final state
            run_state = client.get_run_state(run_id)
            if not run_state.is_successful():
                raise dg.Failure(
                    description=(
                        f"Databricks run {run_id} failed: {run_state.state_message}"
                    ),
                    metadata={
                        "databricks/run_id": run_id,
                        "databricks/state": str(run_state.result_state),
                        "databricks/message": run_state.state_message or "",
                    },
                )

            context.log.info(f"Databricks run {run_id} completed successfully")

            metadata: dict = {
                "databricks/notebook_path": path,
                "databricks/workspace": dg.MetadataValue.url(
                    f"{context.resources.databricks.host}/#job/{run_id}"
                ),
                "databricks/run_id": run_id,
                "databricks/environment": params["ENVIRONMENT"],
            }
            if year and month:
                metadata["partition/year"] = year
                metadata["partition/month"] = month

            return dg.MaterializeResult(metadata=metadata)

        return _notebook_asset
