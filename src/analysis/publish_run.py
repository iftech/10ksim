"""Publish finished runs' metrics to the DST dashboard from their folders."""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from src.analysis.post_run.metrics import run_scrape_config
from src.analysis.post_run.publish import DASHBOARD_URL, TOKEN_ENV, experiment_body, publish
from src.analysis.utils.log_utils import init_logger

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", help="Run folders holding a metadata.json")
    parser.add_argument("--dry-run", action="store_true", help="Print the experiments instead")
    args = parser.parse_args()
    init_logger(logging.getLogger(), verbosity=1)
    token = os.environ.get(TOKEN_ENV)
    if not token and not args.dry_run:
        sys.exit(f"Set {TOKEN_ENV} to a token from {DASHBOARD_URL}/admin/token")

    failed = []
    for arg in args.runs:
        run = Path(arg).resolve()
        exp = json.loads((run / "metadata.json").read_text())
        # Same rule as the post-run scrape, so the dashboard holds what the CSVs hold.
        if not exp.get("results", {}).get("stable", {}).get("duration"):
            logger.error(f"`{run.name}` has no stable window; skipping it")
            failed.append(run.name)
            continue
        body = experiment_body(exp, run_scrape_config(exp, name=run.name), run.name)
        if args.dry_run:
            print(json.dumps(body, indent=2))
            continue
        try:
            publish(body, token)
        except Exception:
            logger.exception(f"Publishing `{run.name}` failed")
            failed.append(run.name)

    if failed:
        sys.exit(f"Not published: {failed}")


if __name__ == "__main__":
    main()
