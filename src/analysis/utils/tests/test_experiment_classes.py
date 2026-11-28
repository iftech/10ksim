import json

import pytest

from src.analysis.utils.experiment_classes import subclass_names
from src.deployments.experiments.libp2p.churn import NodeChurn
from src.deployments.experiments.libp2p.degraded import DegradedNetwork
from src.deployments.experiments.libp2p.nimlibp2p import NimLibp2pExperiment
from src.deployments.experiments.libp2p.partition import NetworkPartition


def test_includes_the_base_class_itself():
    assert NimLibp2pExperiment.__name__ in subclass_names(NimLibp2pExperiment)


@pytest.mark.parametrize("scenario", [DegradedNetwork, NodeChurn, NetworkPartition])
def test_includes_every_scenario_subclass(scenario):
    """These were skipped by an exact name match, so their metrics were never scraped."""
    assert scenario.__name__ in subclass_names(NimLibp2pExperiment)


def test_excludes_unrelated_experiments():
    names = subclass_names(NimLibp2pExperiment)
    assert "WakuExperiment" not in names
    assert "ShadowGossipsubExperiment" not in names


def test_scrape_picks_up_a_scenario_run(tmp_path):
    """End to end: a churn run folder must survive the scrape's class filter."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[4]))  # scrape.py sits at the root
    from scrape import get_nimlibp2p_exps

    run = tmp_path / "churn-run"
    run.mkdir()
    (run / "metadata.json").write_text(
        json.dumps(
            {
                "experiment": {
                    "class": "NodeChurn",
                    "dump": {"_type": "src.deployments.experiments.libp2p.churn.NodeChurn"},
                },
                "stack": {"start_time": "2026-08-05T01:00:07", "end_time": "2026-08-05T01:28:06"},
                "metadata": {"namespace": "zerotesting"},
            }
        )
    )
    found = list(get_nimlibp2p_exps(tmp_path))
    assert len(found) == 1
    assert found[0]["experiment"]["class"] == "NodeChurn"


def test_finds_subclasses_without_importing_them_first(tmp_path):
    """Importing one experiment populates the registry, so a scan-if-empty guard would
    leave the rest unregistered. Run in a clean interpreter to catch that."""
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[4]
    code = (
        "from src.analysis.utils.experiment_classes import subclass_names\n"
        "from src.deployments.experiments.libp2p.nimlibp2p import NimLibp2pExperiment\n"
        "names = subclass_names(NimLibp2pExperiment)\n"
        "assert {'NodeChurn', 'NetworkPartition', 'DegradedNetwork'} <= names, sorted(names)\n"
        "print('ok')\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(root), "PATH": "/usr/bin:/bin"},
    )
    assert out.returncode == 0, out.stderr[-400:]


def test_works_from_any_working_directory(tmp_path, monkeypatch):
    """registry.scan resolves a relative path against the cwd and rglob on a missing
    directory finds nothing silently, so a relative default would quietly accept only
    the base class when scrape.py is run from elsewhere."""
    subclass_names.cache_clear()
    monkeypatch.chdir(tmp_path)
    names = subclass_names(NimLibp2pExperiment)
    assert {"NodeChurn", "NetworkPartition", "DegradedNetwork"} <= names


def test_a_missing_folder_is_an_error_not_an_empty_result():
    subclass_names.cache_clear()
    with pytest.raises(FileNotFoundError):
        subclass_names(NimLibp2pExperiment, folder="/nonexistent/experiments")
