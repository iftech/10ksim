import os
from datetime import timedelta
from enum import StrEnum
from pathlib import Path

# VictoriaLogs and VictoriaMetrics keep 6 months (retentionPeriod=6).
VICTORIA_RETENTION = timedelta(days=180)

DEFAULT_CONFIG_PATH = Path.home() / ".cache" / "dst_dashboard" / "config.yaml"
DEFAULT_DATA_PATH = Path.home() / ".cache" / "dst_dashboard" / "data"
DEFAULT_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_MONGO_DB_NAME = "dst_dashboard"
# Comma-separated list of origins allowed to call this API from a browser.
DEFAULT_ALLOWED_ORIGINS = "https://dashboard.lab.vac.dev"
# Clearly-marked insecure fallback so it's obvious in logs/code review if a real
# secret was never configured - never rely on this outside local dev.
INSECURE_DEFAULT_JWT_SECRET = "dev-only-insecure-secret-change-me"


class Constants(StrEnum):
    """Constants used in DST dashboard application."""

    DST_CONFIG_PATH = os.environ.get(
        "DST_CONFIG_PATH",
        str(DEFAULT_CONFIG_PATH),
    )
    DST_DATA_PATH = os.environ.get(
        "DST_DATA_PATH",
        str(DEFAULT_DATA_PATH),
    )
    DST_MONGO_URI = os.environ.get(
        "DST_MONGO_URI",
        DEFAULT_MONGO_URI,
    )
    DST_MONGO_DB_NAME = os.environ.get(
        "DST_MONGO_DB_NAME",
        DEFAULT_MONGO_DB_NAME,
    )
    DST_JWT_SECRET = os.environ.get(
        "DST_JWT_SECRET",
        INSECURE_DEFAULT_JWT_SECRET,
    )
    DST_ALLOWED_ORIGINS = os.environ.get(
        "DST_ALLOWED_ORIGINS",
        DEFAULT_ALLOWED_ORIGINS,
    )
