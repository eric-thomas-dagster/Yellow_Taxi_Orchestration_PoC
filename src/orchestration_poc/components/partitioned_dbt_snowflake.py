"""Partitioned dbt Snowflake component for NYC Yellow Taxi PoC.

Subclasses the built-in DbtProjectComponent to add:
  - Source-to-Databricks asset key mapping for end-to-end lineage
  - Monthly partitions for orchestration and backfill
  - Configurable group name for dbt assets

The existing dbt SQL is used as-is — dbt handles its own incremental
watermarks (pickup_datetime > max). Dagster provides scheduling, dependency
management, backfill UI, and partition-level observability.
"""

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

import dagster as dg
from dagster_dbt import DbtProjectComponent

_MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2023-01-01",
    end_offset=0,
)


@dataclass
class PartitionedDbtSnowflake(DbtProjectComponent):
    """dbt project with monthly partitions and Databricks lineage.

    Extends DbtProjectComponent to:
    1. Remap dbt source nodes to Databricks ingestion assets (full lineage)
    2. Apply monthly partitions to all dbt assets for backfill support
    3. Set a custom group name for all dbt assets

    Everything else — project resolution, CLI execution, manifest parsing,
    asset check generation — is inherited from DbtProjectComponent.
    """

    group_name: str = ""
    source_key_map: dict[str, str] = field(default_factory=dict)

    def _resolve_source_key(self, dep_asset_key: dg.AssetKey, manifest: Mapping[str, Any], project: Optional[Any]) -> dg.AssetKey | None:
        """Check if a dependency matches a source that should be remapped."""
        for source_id, target_key_str in self.source_key_map.items():
            source_node = manifest.get("sources", {}).get(source_id, {})
            if source_node:
                source_key = super().get_asset_spec(manifest, source_id, project).key
                if dep_asset_key == source_key:
                    # Support slash-separated keys like "schema/table"
                    parts = target_key_str.split("/")
                    return dg.AssetKey(parts)
        return None

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[Any] = None,
    ) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)

        # Remap source dependencies to Databricks ingestion asset keys
        new_deps = []
        for dep in spec.deps:
            remapped_key = self._resolve_source_key(dep.asset_key, manifest, project)
            if remapped_key:
                new_deps.append(dg.AssetDep(asset=remapped_key))
            else:
                new_deps.append(dep)

        attrs: dict[str, Any] = {
            "deps": new_deps,
            "partitions_def": _MONTHLY_PARTITIONS,
        }
        if self.group_name:
            attrs["group_name"] = self.group_name

        return spec.replace_attributes(**attrs)
