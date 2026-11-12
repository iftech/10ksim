"""The 10ksim commit a run deploys from."""

import logging
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

REPO = Path(__file__).parent


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def code_version(repo: Path = REPO) -> Optional[dict]:
    """Commit and branch of `repo`, and whether it was clean and pushed; None without git."""
    try:
        commit = _git(repo, "rev-parse", "HEAD")
        return {
            "repo": _git(repo, "remote", "get-url", "origin"),
            "commit": commit,
            "branch": _git(repo, "rev-parse", "--abbrev-ref", "HEAD"),
            "clean": not _git(repo, "status", "--porcelain", "--untracked-files=no"),
            "pushed": bool(_git(repo, "branch", "--remotes", "--contains", commit)),
        }
    except (OSError, subprocess.CalledProcessError) as e:
        logger.warning(f"Could not read the 10ksim commit: {e}")
        return None
