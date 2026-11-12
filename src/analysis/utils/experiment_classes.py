"""Which experiment classes a metadata filter should accept.

Run metadata records the class by bare name, so filtering on one name silently skips
every subclass. The scenarios subclass NimLibp2pExperiment, which is how the metrics
scrape came to ignore them entirely. Deriving the set from the registry keeps new
subclasses included without anyone remembering to update a list.

Matching is by class name rather than the dotted `_type` in the dump, because `_type`
records the module a run was launched from and goes stale when a module is renamed.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import FrozenSet, Type, Union

from src.deployments.registry import registry

logger = logging.getLogger(__name__)

# Anchored to this file, not the working directory: registry.scan resolves a relative
# path against the cwd and rglob on a missing directory finds nothing without erroring,
# so a relative default would silently accept only the base class from anywhere else.
EXPERIMENTS_FOLDER = Path(__file__).resolve().parents[3] / "src" / "deployments" / "experiments"


@lru_cache(maxsize=None)
def subclass_names(base: Type, *, folder: Union[str, Path] = EXPERIMENTS_FOLDER) -> FrozenSet[str]:
    """Names of every registered experiment class that is `base` or a subclass of it."""
    folder = Path(folder)
    if not folder.is_dir():
        raise FileNotFoundError(f"Experiments folder not found for class scan: `{folder}`")
    # Always scan: importing one experiment module populates the registry, so a
    # "scan only if empty" guard leaves every other subclass unregistered.
    registry.scan(str(folder), mode="skip")
    names = {
        info.cls.__name__
        for info in registry.items()
        if isinstance(info.cls, type) and issubclass(info.cls, base)
    }
    names.add(base.__name__)
    logger.debug(f"Accepting experiment classes for {base.__name__}: {sorted(names)}")
    return frozenset(names)
