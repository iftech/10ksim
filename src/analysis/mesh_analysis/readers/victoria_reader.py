# Python Imports
import json
import logging
import re
from typing import Dict, Iterator, List, Optional, Tuple

import pandas as pd
import requests
from result import Err, Ok, Result

# Project Imports
from src.analysis.mesh_analysis.readers.reader import Reader
from src.analysis.mesh_analysis.readers.tracers.message_tracer import MessageTracer

logger = logging.getLogger(__name__)


class VictoriaReader(Reader):
    def __init__(
        self,
        tracer: Optional[MessageTracer],
        victoria_config_query: Dict,
        extra_fields: Optional[List[str]] = None,
    ):
        """
        :param tracer: MessageTracer instance to retrieve raw message patterns from Victoria.
        :param victoria_config_query: Configuration for the Victoria query. This allows to do a first filtering by the
        monitoring stack retrieving only the lines we are interested in, saving time in the parsing process.
        """
        self._tracer: MessageTracer = tracer
        self._config_query = victoria_config_query

    def _fetch_data(self, url: str, headers: Dict, params: Dict, extra_fields: List[str]):
        logs = []
        logger.debug(f"Fetching {params}")
        with requests.post(url=url, headers=headers, params=params, stream=True) as response:
            for line in response.iter_lines():
                if line:
                    try:
                        parsed_object = json.loads(line)
                    except json.decoder.JSONDecodeError as e:
                        logger.info(line)
                        exit()
                    try:
                        logs.append(
                            (parsed_object["_msg"],) + tuple(parsed_object[k] for k in extra_fields)
                        )
                    except KeyError as e:
                        logger.warning(f"Malformed log line skipped due to missing key {e}: {line}")
                        continue
        logger.debug(f"Fetched {len(logs)} log lines")

        return logs

    def make_queries(self) -> List[List[Tuple]]:
        """
        This function returns a list of lists, structured hierarchically as follows:
        - The outer list corresponds to the pattern groups.
        - Each element in the outer list is a list representing a specific pattern group.
        - Within each pattern group list, there are sublists, one for each pattern in that group.
        - Each sublist contains the lines that match the corresponding pattern.

        The result is organized as [pattern_groups -> patterns -> matched_lines], where:
        - Each pattern group can have multiple patterns.
        - Each pattern can match multiple lines.
        """
        params = self._config_query["params"]
        if isinstance(params, Dict):
            params = [params]

        results = [[] for _ in self._tracer.patterns]
        for i, pattern_group in enumerate(self._tracer.patterns):
            logs = self._fetch_data(
                self._config_query["url"],
                self._config_query["headers"],
                params[i],
                self._tracer.extra_fields,
            )
            query_results = [[] for _ in pattern_group.trace_pairs]
            for log_line in logs:
                for j, trace_pair in enumerate(pattern_group.trace_pairs):
                    pattern = trace_pair.regex
                    match = re.search(pattern, log_line[0])
                    if match:
                        match_as_list = list(match.groups())
                        match_as_list.extend(log_line[1:])
                        query_results[j].append(match_as_list)

            results[i].extend(query_results)

        return results

    def get_dataframes(self) -> Dict[str, List[pd.DataFrame]]:
        results = self.make_queries()
        dfs = self._tracer.trace(results)

        return dfs

    def single_query_info(self) -> Result[Dict, requests.Response]:
        response = requests.post(**self._config_query)
        if response.status_code != 200:
            logger.error(f"Request failed with status code: {response.status_code}")
            return Err(response)

        try:
            data = response.json()
            return Ok(data)
        except json.decoder.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            logger.error(f"Response content: {response.content}")

            return Err(response)

    def multiline_query_info(self) -> Result[Iterator, str]:
        response = requests.post(
            self._config_query["url"],
            headers=self._config_query["headers"],
            params=self._config_query["params"],
        )
        if response.status_code != 200:
            logger.error(f"Request failed with status code: {response.status_code}")
            return Err(response.text)

        try:
            data = response.iter_lines()
            return Ok(data)
        except json.decoder.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            logger.error(f"Response content: {response.content}")

            return Err(response.text)
