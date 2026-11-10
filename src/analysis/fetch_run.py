"""Put published runs' metrics in their run folders, unless they were scraped there."""

import argparse
import logging
import sys
from pathlib import Path

from src.analysis.post_run.fetch import run_metrics
from src.analysis.utils.log_utils import init_logger

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", help="Run folders, named as the run on the dashboard")
    args = parser.parse_args()
    init_logger(logging.getLogger(), verbosity=1)

    failed = []
    for arg in args.runs:
        run = Path(arg).resolve()
        try:
            print(run_metrics(run))
        except Exception:
            logger.exception(f"Fetching `{run.name}` failed")
            failed.append(run.name)

    if failed:
        sys.exit(f"Not fetched: {failed}")


if __name__ == "__main__":
    main()
