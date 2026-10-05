# Python Imports
import json
import logging
import traceback
from functools import partial
from pathlib import Path
from typing import Callable, List, Literal, Optional, Self

import seaborn as sns
from pydantic import BaseModel

# Project Imports
from src.analysis.mesh_analysis.analyzers.data_puller import DataPuller
from src.analysis.utils import path_utils

logger = logging.getLogger(__name__)
sns.set_theme()


class AnalysisResult(BaseModel):
    name: str
    intermediates: dict
    """Add extra information for results.
    For example:
    ```
        intermediates={
            "ss_nodes": ss_nodes,
            **({"failed": error_message} if not passed else {}),
        },
    ```
    """
    status: Literal["passed", "failed", "skipped", "error"]

    def get_metric(self, metric) -> Optional[object]:
        return self.intermediates.get(metric, None)


OnFail = Literal["stop", "continue"]


class AnalysisStep(BaseModel):
    name: str
    action: Callable[[], AnalysisResult]
    on_fail: OnFail = "continue"
    kwargs: Optional[dict]

    def run(self) -> AnalysisResult:
        try:
            logger.info(f"Running {self.name}")
            return self.action(**self.kwargs)
        except Exception as e:
            full_trace = traceback.format_exc()
            logger.error(f"exception: {e}\n{full_trace}")
            return AnalysisResult(
                name=self.name,
                intermediates={"error": e, "trace": full_trace},
                status="error",
            )


class Analyzer(BaseModel):
    _analysis_steps: List[AnalysisStep] = []
    data_puller: Optional[DataPuller] = None
    dump_analysis_dir: Optional[Path] = None

    def run(self) -> List[AnalysisResult]:
        self._set_up_paths()
        results = []
        for analysis in self._analysis_steps:
            result = analysis.run()
            if result.status in ["error", "failed"] and analysis.on_fail != "continue":
                dump = json.dumps(result.model_dump(indent=False), indent=2)
                raise ValueError(f"Analysis failed. {dump}")
            results.append(result)

        return results

    def _with_parameterized_check(
        self,
        func,
        *,
        name: Optional[str] = None,
        on_fail: OnFail = "continue",
        **params,
    ) -> Self:
        if name is None:
            name = func.__name__
        func = partial(func, **params)
        self._analysis_steps.append(
            AnalysisStep(name=name, action=func, kwargs=params, on_fail=on_fail)
        )
        return self

    def with_dump_analysis_dir(self, dump_analysis_dir: str = None) -> Self:
        self.dump_analysis_dir = Path(dump_analysis_dir)
        return self

    def with_data_puller(self, data_puller: DataPuller) -> Self:
        self.data_puller = data_puller
        return self

    def _set_up_paths(self):
        try:
            self._dump_analysis_path = (
                Path(self.dump_analysis_dir) if self.dump_analysis_dir else None
            )
            result = path_utils.prepare_path_for_folder(self._dump_analysis_path)
            if result.is_err():
                logger.error(result.err_value)
                exit(1)
        except Exception as e:
            full_trace = traceback.format_exc()
            logger.error(f"Failure setting up paths. exception: {e}\n{full_trace}")
