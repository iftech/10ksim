# Python Imports
import logging
import multiprocessing
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd

# Project Imports
from src.analysis.mesh_analysis.readers.reader import Reader
from src.analysis.mesh_analysis.readers.tracers.message_tracer import MessageTracer
from src.analysis.utils import file_utils

logger = logging.getLogger(__name__)


def merge_logs_per_pattern(tracer: MessageTracer, files_logs) -> List:
    result = []

    for group_idx, group in enumerate(tracer.patterns):
        logs = []

        for pattern_idx in range(len(group.trace_pairs)):
            all_logs = []
            for file_logs in files_logs:
                all_logs.extend(file_logs[group_idx][pattern_idx])
            logs.append(all_logs)
        result.append(logs)

    return result


class FileReader(Reader):
    def __init__(self, folder: Path, tracer: MessageTracer, n_jobs: int):
        self._folder_path = folder
        self._tracer = tracer
        self._n_jobs = n_jobs

    def get_dataframes(self) -> List[Dict[str, List[pd.DataFrame]]]:
        logger.info(f"Reading {self._folder_path}")
        files_result = file_utils.get_files_from_folder_path(self._folder_path, extension="*.log")

        if files_result.is_err():
            raise OSError(f"Could not read {self._folder_path}: {files_result.err_value}")

        parsed_logs = self._read_files(files_result.ok_value)
        logger.info(f"Tracing {self._folder_path}")

        dfs = [self._tracer.trace(logs) for logs in parsed_logs]
        return dfs

    def _read_files(self, files: List) -> List:
        with multiprocessing.Pool(processes=self._n_jobs) as pool:
            parsed_logs = pool.map(self._read_file_patterns, files)

        return parsed_logs

    def _read_file_patterns(self, file: str) -> List:
        results = [[] for p in self._tracer.patterns]

        with open(Path(self._folder_path) / file) as log_file:
            lines = log_file.readlines()
            # TODO: Potential for optimizations for reading here.

        for i, pattern_group in enumerate(self._tracer.patterns):
            query_results = [[] for _ in pattern_group.trace_pairs]

            for line in lines:
                for j, trace_pair in enumerate(pattern_group.trace_pairs):
                    match = re.search(trace_pair.regex, line)
                    if match:
                        match_as_list = list(match.groups())
                        match_as_list.append(file)
                        query_results[j].append(match_as_list)
                        break

            results[i].extend(query_results)

        return results
