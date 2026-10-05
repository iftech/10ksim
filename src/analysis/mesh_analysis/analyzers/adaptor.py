import json
import logging
from pathlib import Path
from typing import Optional, Self

from pydantic import BaseModel

from src.deployments.experiments.base_experiment import BaseExperiment

logger = logging.getLogger(__name__)


def victoria() -> dict:
    return {
        "url": "https://vlselect.lab.vac.dev/select/logsql/query",
        "type": "vaclab",
        "reader": "victoria",
    }


def read_experiment(exp: BaseExperiment):
    with open(exp.metadata_log_path, "r", encoding="utf-8") as f:
        return json.load(f)


class AnalysisAdaptor(BaseModel):
    """Provides a friendlier interface for building Analyzers by automatically extracting experiment metadata, creating the DataPuller, and handling metadata->analysis_step_params logic."""

    _metadata: Optional[dict] = None

    def with_metadata(self, exp: BaseExperiment | dict) -> Self:
        if isinstance(exp, BaseExperiment):
            exp = exp.metadata
        if not self._metadata:
            self._metadata = {}
        self._metadata.update(exp)
        self.supports(self._metadata["experiment"]["name"])
        self.data_puller.with_kwargs(exp["stack"])
        try:
            output_folder = Path(self._metadata["metadata"]["args"]["output_folder"])
            self.with_dump_analysis_dir(output_folder / "analysis_data")
        except (TypeError, KeyError):
            pass
        return self

    def with_local(self, folder: str) -> Self:
        self.data_puller.with_local(folder)
        return self

    def with_vaclab(self, url: Optional[str] = None) -> Self:
        args = victoria()
        if url:
            args.update({"url": url})
        self.data_puller.with_kwargs(args)
        return self
