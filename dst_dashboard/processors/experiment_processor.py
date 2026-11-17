"""Experiment processor - processes complete experiments with datasets and panels."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from bson import ObjectId

from dst_dashboard.config.data_structures import (
    DashboardFullConfig,
    DatasetConfig,
    ExperimentConfig,
)
from dst_dashboard.processors.panel_processor import PanelProcessor
from dst_dashboard.storage.db import DSTDatabase

logger = logging.getLogger(__name__)


class ExperimentProcessor(PanelProcessor):
    """
    Experiment processor - top-level processor.
    """

    def __init__(self, config: DashboardFullConfig, db: DSTDatabase):
        super().__init__(config, db)

    def _ensure_experiment_id(self, experiment: ExperimentConfig) -> str:
        """Ensure experiment has a valid ID, generating one (Mongo ObjectId-style) if missing."""
        if not experiment.id or experiment.id.strip() == "":
            generated_id = str(ObjectId())
            logger.info(f"Generated ID for experiment '{experiment.title}': {generated_id}")
            experiment.id = generated_id

        return experiment.id

    def process_dataset(self, experiment_id: str, dataset_config: DatasetConfig) -> bool:
        """Process a single dataset - fetch and store if needed. Returns True on success."""
        logger.info(f"Processing dataset: {dataset_config.name}")

        existing_data = self.db.get_dataset(experiment_id, dataset_config.name)
        if existing_data is not None:
            logger.info(
                f"Dataset '{dataset_config.name}' already exists with {len(existing_data)} rows, skipping fetch"
            )
            return True

        # Fetch and store dataset
        try:
            logger.info(
                f"Fetching dataset '{dataset_config.name}' from {dataset_config.datasource}"
            )
            data = self.fetch_dataset(experiment_id, dataset_config)

            if data:
                self.db.store_dataset(experiment_id, dataset_config.name, data)
                logger.info(f"Stored {len(data)} rows for dataset '{dataset_config.name}'")
                return True
            # Left unstored, so the next reprocess fetches it again.
            logger.warning(f"No data fetched for dataset '{dataset_config.name}'")
            return False

        except Exception as e:
            logger.error(f"Failed to fetch dataset '{dataset_config.name}': {e}")
            return False

    def process_experiment_datasets(
        self, experiment: ExperimentConfig, max_workers: int = 4
    ) -> int:
        """Process all datasets for an experiment concurrently (fetches are I/O-bound). Returns the number processed successfully."""
        if not experiment.datasets:
            return 0

        if len(experiment.datasets) == 1 or max_workers <= 1:
            return sum(
                self.process_dataset(experiment.id, dataset_config)
                for dataset_config in experiment.datasets
            )

        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.process_dataset, experiment.id, dataset_config): dataset_config
                for dataset_config in experiment.datasets
            }
            for future in as_completed(futures):
                dataset_config = futures[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception:
                    logger.error(
                        f"Dataset '{dataset_config.name}' processing raised unexpectedly",
                        exc_info=True,
                    )

        return success_count

    def process_experiment(self, experiment: ExperimentConfig) -> str:
        """Process a complete experiment - store it, fetch datasets, store panels. Returns the experiment ID."""
        experiment_id = self._ensure_experiment_id(experiment)

        logger.info(f"Processing experiment: {experiment_id} - {experiment.title}")

        # 1. Store experiment in database
        experiment_dict = experiment.model_dump()
        existing_exp = self.db.get_experiment(experiment_id)

        if existing_exp:
            logger.info(f"Experiment '{experiment_id}' already exists in database, updating...")
        else:
            logger.info(f"Storing new experiment '{experiment_id}' in database")

        self.db.store_experiment(experiment_dict)

        # 2. Process datasets (datasets depend on datasources)
        dataset_count = self.process_experiment_datasets(experiment)
        logger.info(
            f"Processed {dataset_count}/{len(experiment.datasets)} datasets for experiment '{experiment_id}'"
        )

        # 3. Process panels (panels depend on datasets)
        panel_count = self.process_experiment_panels(experiment)
        logger.info(
            f"Processed {panel_count}/{len(experiment.panels)} panels for experiment '{experiment_id}'"
        )

        return experiment_id
