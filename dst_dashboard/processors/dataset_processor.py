"""Dataset processor - base class for fetching and processing datasets."""

import logging
import threading
from typing import Any, Dict, List, Optional

import pandas as pd
from result import Err, Ok

from dst_dashboard.config.data_structures import (
    DashboardFullConfig,
    DatasetConfig,
    DataSourceConfig,
    ExperimentConfig,
)
from dst_dashboard.storage.db import DSTDatabase
from src.analysis.mesh_analysis.analyzers.data_puller import DataPuller
from src.analysis.mesh_analysis.readers.tracers.message_tracer import MessageTracer
from src.analysis.metrics import scrape_utils

logger = logging.getLogger(__name__)

VM_QUERY_TIMEOUT_S = 120

_victorialogs_log_init_lock = threading.Lock()
_victorialogs_log_initialized = False


def _ensure_victorialogs_logging_initialized():
    """One-time (per-process) init of the log queue DataPuller's worker processes attach to.

    log_utils.apply_config() mutates global logging state (clears every
    logger's handlers, restarts a QueueListener), so it must only ever run
    once - concurrent dataset fetches (now run via a thread pool) would
    otherwise race on that global state.
    """
    global _victorialogs_log_initialized
    if _victorialogs_log_initialized:
        return
    with _victorialogs_log_init_lock:
        if _victorialogs_log_initialized:
            return
        from src.analysis.utils.log_utils import Config, apply_config

        try:
            config = Config(
                logger_name="data_puller",
                level=logging.INFO,
                fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[],
                tz_offset_hours=0,
            )
            apply_config(config)
            logger.debug("Log queue initialized for DataPuller")
        except Exception as e:
            logger.debug(f"Log queue initialization skipped: {e}")
        _victorialogs_log_initialized = True


class DatasetProcessor:
    """
    Base processor for datasets - fetches data using DataPuller or scrape_utils.

    This is the base class in the processor hierarchy:
    ExperimentProcessor -> PanelProcessor -> DatasetProcessor
    """

    def __init__(self, config: DashboardFullConfig, db: DSTDatabase):
        self.config = config
        self.db = db
        self._datasources = {ds.name: ds for ds in config.datasources}

    def _get_datasource(self, datasource_name: str) -> Optional[DataSourceConfig]:
        """Get datasource configuration by name."""
        return self._datasources.get(datasource_name)

    def _get_experiment(self, experiment_id: str) -> Optional[ExperimentConfig]:
        """Get experiment configuration by ID. Experiments live only in the database."""
        experiment_data = self.db.get_experiment(experiment_id)
        if experiment_data is None:
            return None
        return ExperimentConfig(**experiment_data)

    def _apply_schema(
        self, data_rows: List[Dict[str, Any]], dataset_config: DatasetConfig
    ) -> List[Dict[str, Any]]:
        """Apply schema to data rows - filter fields and apply type conversions."""
        if not dataset_config.schema:
            return data_rows

        schema_fields = {field.name: field.type for field in dataset_config.schema}
        filtered_rows = []

        for row in data_rows:
            filtered_row = {}
            for field_name, field_type in schema_fields.items():
                if field_name not in row:
                    continue

                value = row[field_name]

                # Apply type conversions
                try:
                    if field_type == "datetime":
                        if not isinstance(value, pd.Timestamp):
                            value = pd.to_datetime(value)
                    elif field_type == "float":
                        value = float(value)
                    elif field_type == "int":
                        value = int(value)
                    elif field_type == "string":
                        value = str(value)

                    filtered_row[field_name] = value
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to convert field '{field_name}' to {field_type}: {e}")
                    continue

            if filtered_row:  # Only add if we have at least some fields
                filtered_rows.append(filtered_row)

        return filtered_rows

    def fetch_dataset(
        self, experiment_id: str, dataset_config: DatasetConfig
    ) -> List[Dict[str, Any]]:
        """Fetch dataset using the appropriate fetcher based on datasource type."""
        datasource = self._get_datasource(dataset_config.datasource)
        if not datasource:
            raise ValueError(
                f"Datasource '{dataset_config.datasource}' not found for dataset '{dataset_config.name}'"
            )

        experiment = self._get_experiment(experiment_id)
        if not experiment:
            raise ValueError(f"Experiment '{experiment_id}' not found")

        try:
            if datasource.type == "VictoriaLogs":
                return self._fetch_from_victorialogs(dataset_config, datasource, experiment)
            elif datasource.type == "Prometheus":
                return self._fetch_from_prometheus(dataset_config, datasource, experiment)
            else:
                raise ValueError(f"Unsupported datasource type: {datasource.type}")
        except Exception as e:
            logger.error(
                f"Failed to fetch dataset '{dataset_config.name}' from {datasource.type}: {e}"
            )
            raise

    def _get_tracer_for_dataset(self, dataset_config: DatasetConfig):
        """Create the appropriate tracer based on dataset configuration."""
        tracer_type = dataset_config.query.tracer
        pattern = dataset_config.query.pattern

        # Extract kubernetes fields from schema that need to be fetched as extra_fields
        extra_fields = []
        for field in dataset_config.schema:
            # Map common schema field names to kubernetes field names
            if field.name == "pod_name":
                extra_fields.append("kubernetes.pod_name")
            elif field.name == "namespace":
                extra_fields.append("kubernetes.namespace")
            elif field.name == "container_name":
                extra_fields.append("kubernetes.container_name")
            elif field.name.startswith("kubernetes."):
                extra_fields.append(field.name)

        if tracer_type == "nimlibp2p":
            from src.analysis.mesh_analysis.readers.tracers.nimlibp2p_tracer import Nimlibp2pTracer

            tracer = Nimlibp2pTracer()

            if extra_fields:
                tracer = tracer.with_extra_fields(extra_fields)

            # Use specific pattern if provided
            if pattern == "received":
                tracer = tracer.with_received_pattern_group()
            elif pattern == "sent":
                tracer = tracer.with_sent_pattern_group()
            else:
                # Default to received for message delay analysis
                tracer = tracer.with_received_pattern_group()

            return tracer
        else:
            # Fallback to generic MessageTracer. MessageTracer only supports a
            # wildcard pattern today, so `pattern` isn't applicable here.
            tracer = MessageTracer()
            tracer.with_wildcard_pattern()
            return tracer

    def _fetch_from_victorialogs(
        self,
        dataset_config: DatasetConfig,
        datasource: DataSourceConfig,
        experiment: ExperimentConfig,
    ) -> List[Dict[str, Any]]:
        """Fetch logs from VictoriaLogs using DataPuller."""
        logger.info(f"Fetching dataset '{dataset_config.name}' from VictoriaLogs")

        # Get statefulsets and nodes from experiment metadata
        stateful_sets = experiment.metadata.get("statefulsets", [])
        nodes_per_ss = experiment.metadata.get("nodes", [])

        if not stateful_sets or not nodes_per_ss:
            logger.warning(
                f"Experiment '{experiment.id}' missing statefulsets or nodes in metadata - returning empty dataset"
            )
            return []

        # Build kwargs for DataPuller
        kwargs = {
            "url": datasource.url,
            "start_time": dataset_config.timeRange.start.isoformat(),
            "end_time": dataset_config.timeRange.end.isoformat(),
            "extra_fields": [field.name for field in dataset_config.schema],
            "namespace": dataset_config.query.namespace or experiment.metadata.get("namespace", ""),
            "stateful_sets": stateful_sets,
            "nodes_per_statefulset": nodes_per_ss,
            "container_name": experiment.metadata.get("container_name", ""),
        }

        # Get tracer based on configuration
        tracer = self._get_tracer_for_dataset(dataset_config)

        _ensure_victorialogs_logging_initialized()

        # Initialize DataPuller
        data_puller = DataPuller().with_kwargs(kwargs).with_source_type("victoria")

        try:
            logger.debug(f"DataPuller kwargs: {kwargs}")
            logger.debug(
                f"Fetching with stateful_sets={stateful_sets}, nodes_per_ss={nodes_per_ss}"
            )

            # Fetch dataframes from VictoriaLogs
            results = data_puller.get_all_node_dataframes(tracer, stateful_sets, nodes_per_ss)

            logger.debug(f"DataPuller returned {len(results)} result dicts")

            # Convert results to standard format
            data_rows = []
            for idx, result_dict in enumerate(results):
                logger.debug(f"Processing result_dict {idx}: {list(result_dict.keys())}")
                for pattern_name, df_list in result_dict.items():
                    logger.debug(f"Pattern '{pattern_name}': {len(df_list)} dataframes")
                    for df_idx, df in enumerate(df_list):
                        if isinstance(df, pd.DataFrame):
                            if not df.empty:
                                logger.debug(
                                    f"DataFrame {df_idx} shape: {df.shape}, columns: {list(df.columns)}"
                                )
                                # Convert DataFrame to list of dicts
                                df_dict = df.reset_index().to_dict(orient="records")
                                data_rows.extend(df_dict)
                            else:
                                logger.debug(f"DataFrame {df_idx} is empty")
                        elif isinstance(df, list):
                            # Handle list format directly
                            logger.debug(f"List {df_idx} has {len(df)} items")
                            if df:  # If list is not empty
                                # Check if items are already dicts
                                if isinstance(df[0], dict):
                                    data_rows.extend(df)
                                else:
                                    logger.warning(f"List contains non-dict items: {type(df[0])}")
                        else:
                            logger.warning(f"Result is not a DataFrame or list: {type(df)}")

            logger.info(f"Converted {len(data_rows)} raw rows from VictoriaLogs")

            # Normalize kubernetes.* field names (e.g., kubernetes.pod_name -> pod_name)
            normalized_rows = []
            for row in data_rows:
                normalized_row = {}
                for key, value in row.items():
                    if key.startswith("kubernetes."):
                        # Remove 'kubernetes.' prefix
                        new_key = key.replace("kubernetes.", "")
                        normalized_row[new_key] = value
                    else:
                        normalized_row[key] = value
                normalized_rows.append(normalized_row)

            logger.debug(f"Normalized {len(normalized_rows)} rows (renamed kubernetes.* fields)")

            # Apply schema filtering and type conversions
            filtered_rows = self._apply_schema(normalized_rows, dataset_config)

            logger.info(
                f"Fetched {len(data_rows)} rows from VictoriaLogs, "
                f"{len(filtered_rows)} rows after schema filtering for dataset '{dataset_config.name}'"
            )
            return filtered_rows

        except Exception as e:
            logger.error(f"Error fetching from VictoriaLogs: {e}", exc_info=True)
            return []

    def _fetch_from_prometheus(
        self,
        dataset_config: DatasetConfig,
        datasource: DataSourceConfig,
        experiment: ExperimentConfig,
    ) -> List[Dict[str, Any]]:
        """Fetch metrics from Prometheus using direct query."""
        logger.info(f"Fetching dataset '{dataset_config.name}' from Prometheus")

        if not dataset_config.query.expr:
            raise ValueError(
                f"Dataset '{dataset_config.name}' missing required 'expr' for Prometheus query"
            )

        # Parse step to integer (remove 's' suffix if present)
        step = dataset_config.query.step or "15s"
        step_value = int(step.rstrip("s"))

        # Create PromQL query URL
        promql_url = scrape_utils.create_promql(
            datasource.url,
            dataset_config.query.expr,
            dataset_config.timeRange.start,
            dataset_config.timeRange.end,
            step_value,
        )

        logger.debug(f"Prometheus query URL: {promql_url}")

        try:
            # Execute query
            result = scrape_utils.get_query_data(promql_url, timeout=VM_QUERY_TIMEOUT_S)

            match result:
                case Ok(data):
                    logger.info(
                        f"Successfully fetched data from Prometheus for dataset '{dataset_config.name}'"
                    )

                    # Convert Prometheus response to standard format
                    data_rows = []
                    for series in data["data"]["result"]:
                        metric_labels = series.get("metric", {})
                        values = series.get("values", [])

                        for timestamp, value in values:
                            # Build row with timestamp, value, and all metric labels
                            row = {
                                "timestamp": pd.to_datetime(timestamp, unit="s"),
                                "value": float(value),
                            }
                            # Add all metric labels as potential fields
                            row.update(metric_labels)
                            data_rows.append(row)

                    # Apply schema filtering and type conversions
                    filtered_rows = self._apply_schema(data_rows, dataset_config)

                    logger.info(
                        f"Fetched {len(data_rows)} rows from Prometheus, "
                        f"{len(filtered_rows)} rows after schema filtering for dataset '{dataset_config.name}'"
                    )
                    return filtered_rows

                case Err(error):
                    logger.error(f"Prometheus query error: {error}")
                    return []

        except Exception as e:
            logger.error(f"Error fetching from Prometheus: {e}")
            return []
