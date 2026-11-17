# Pyton Imports
import json
import logging
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict

from result import Err, Ok, Result

from src.analysis.utils.time_utils import to_utc_timestamp

logger = logging.getLogger(__name__)


def create_promql(
    address: str, query: str, start_scrape: datetime, finish_scrape: datetime, step: int
) -> str:
    query = urllib.parse.quote(query)
    promql = address + "query_range?query=" + query

    start = to_utc_timestamp(start_scrape)
    end = to_utc_timestamp(finish_scrape)

    promql = promql + "&start=" + str(start) + "&end=" + str(end) + "&step=" + str(step)

    return promql


def get_query_data(request: str) -> Result[Dict, str]:
    response = urllib.request.urlopen(request, timeout=30)

    logger.debug(f"Response: {response.status}")
    if response.status != 200:
        return Err(
            f"Error in query. Status code {response.status}. {response.read().decode('utf-8')}"
        )

    logger.debug(f"Response: {response.status}")
    json_response = json.loads(str(response.read(), "utf-8"))

    if len(json_response["data"]["result"]) == 0:
        return Err(f"Returned data is empty.")

    return Ok(json_response)
