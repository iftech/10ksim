# Python Imports
import logging
from collections import defaultdict
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel

# Project Imports
from src.deployments.core.event_log import find_events, parse_events_log
from src.deployments.core.event_mapping import EventMapping
from src.deployments.core.metadata_times import (
    apply_time_shifts,
    enrich_intervals,
    format_metadata_timestamps,
)

logger = logging.getLogger(__name__)

PROJ_ROOT = Path(__file__).parent.parent.parent


class BaseBridge(BaseModel):
    statefulsets_key: str = "stateful_sets"
    nodes_key: str = "nodes_per_statefulset"

    def _get_metadata_from_events_list(
        self,
        events_log_path: Path,
        events_list: List[EventMapping],
        *,
        namespace: Optional[str] = None,
    ) -> dict:
        events_maps = [(obj.key, obj.target) for obj in events_list]
        metadata = parse_events_log(events_log_path, events_maps)

        deltatime_map = {obj.target: obj.time_shift for obj in events_list}
        metadata = apply_time_shifts(deltatime_map, metadata)

        metadata = enrich_intervals(metadata, namespace=namespace)
        metadata = format_metadata_timestamps(metadata, format="vquery")

        return metadata

    def get_metadata(self, events_log: Path) -> dict:
        all_metadata = self._aggregate_metadata_events(events_log)

        # Extract from all metadata and put into the following structure.
        map = {
            "stack": [self.statefulsets_key, self.nodes_key, "namespace"],
            "experiment": [("experiment_name", "name"), ("experiment_class", "class")],
            "metadata": ["command", "kube_config", "namespace", "namespaces", "args"],
        }
        metadata = defaultdict(dict)
        for main_key, sub_keys in map.items():
            for sub_key in sub_keys:
                if isinstance(sub_key, tuple):
                    try:
                        metadata[main_key][sub_key[1]] = all_metadata[sub_key[0]]
                    except KeyError:
                        pass
                else:
                    try:
                        metadata[main_key][sub_key] = all_metadata[sub_key]
                    except KeyError:
                        pass

        metadata["experiment"]["bridge_class"] = {
            "__type__": f"{self.__class__.__module__}.{self.__class__.__name__}",
            **self.model_dump(),
        }
        metadata["stack"]["extra_fields"] = [
            "kubernetes.pod_name",
            "kubernetes.pod_node_name",
        ]
        try:
            metadata["metadata"]["subdir"] = events_log.relative_to(PROJ_ROOT).as_posix()
        except ValueError as e:
            logger.info(e)
        metadata["stack"]["name"] = self._get_name(metadata)
        for key in ("params", "code"):
            if key in all_metadata:
                metadata[key] = all_metadata[key]
        return dict(metadata)

    def _get_name(self, metadata: dict):
        counts = metadata["stack"][self.nodes_key]
        sets = metadata["stack"][self.statefulsets_key]
        nodes_str = "__".join(f"{set}_{count}" for set, count in zip(sets, counts))
        return f"{metadata['experiment']['name']}__{nodes_str}"

    def _aggregate_metadata_events(self, events_log: Path) -> dict:
        """Collect all metadata from events log, and gather StatefulSet deployments."""
        metadata = {}
        namespaces = set()

        # Create list of all deployed StatefulSets to plug into analysis script.
        metadata[self.statefulsets_key] = []
        metadata[self.nodes_key] = []
        for event in find_events(
            events_log, {"event": "deployment", "phase": "start", "kind": "StatefulSet"}
        ):
            metadata[self.statefulsets_key].append(event["name"])
            metadata[self.nodes_key].append(event["replicas"])
            namespaces.add(event["namespace"])

        if len(namespaces) == 1:
            metadata["namespace"] = next(iter(namespaces))
        elif len(namespaces) == 0:
            logger.info(f"No namespaces used.")
        else:
            logger.warning(f"Multiple namespaces used. namespaces: `{namespaces}`")
            metadata["namespaces"] = list(namespaces)

        for event in find_events(events_log, {"event": "metadata"}):
            metadata.update(event)

        return metadata
