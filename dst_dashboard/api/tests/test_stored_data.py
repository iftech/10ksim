from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from dst_dashboard.api import admin, experiments
from dst_dashboard.config.data_structures import ExperimentConfig


def _experiment(start: datetime) -> dict:
    dataset = {
        "datasource": "victoria-metrics",
        "timeRange": {
            "start": start.isoformat(),
            "end": (start + timedelta(minutes=1)).isoformat(),
        },
        "query": {"expr": "up", "step": "15s"},
        "schema": [],
    }
    return {
        "id": "e1",
        "title": "run",
        "family": "nimlibp2p",
        "metadata": {},
        "datasets": [{**dataset, "name": "full"}, {**dataset, "name": "empty"}],
        "panels": [],
        "publish": False,
    }


@pytest.mark.parametrize("rows, kept", [(5, True), (0, False)])
def test_a_create_is_kept_only_if_every_dataset_has_rows(monkeypatch, rows, kept):
    db = MagicMock()
    db.experiments.find_one.return_value = None
    db.get_dataset_metadata.side_effect = lambda _, name: {
        "row_count": rows if name == "empty" else 5
    }
    monkeypatch.setattr(experiments, "DSTDatabase", lambda: db)
    monkeypatch.setattr(experiments, "get_processor", lambda request: MagicMock())
    experiment = ExperimentConfig(**_experiment(datetime(2026, 9, 16, 10, 10)))

    if kept:
        assert experiments.create_experiment(experiment, request=None) is experiment
        db.delete_experiment.assert_not_called()
    else:
        with pytest.raises(HTTPException) as error:
            experiments.create_experiment(experiment, request=None)
        assert error.value.status_code == 502 and "empty" in error.value.detail
        db.delete_experiment.assert_called_once_with(experiment.id)


@pytest.mark.parametrize("age_days, cleared", [(30, True), (200, False)])
def test_a_dataset_past_retention_is_not_cleared(monkeypatch, age_days, cleared):
    start = (datetime.now(timezone.utc) - timedelta(days=age_days)).replace(tzinfo=None)
    db = MagicMock()
    db.get_experiment.return_value = _experiment(start)
    monkeypatch.setattr(admin, "DSTDatabase", lambda: db)

    if cleared:
        assert admin.clear_dataset("e1", "full")["status"] == "success"
        db.delete_dataset.assert_called_once_with("e1", "full")
    else:
        with pytest.raises(HTTPException) as error:
            admin.clear_dataset("e1", "full")
        assert error.value.status_code == 409
        db.delete_dataset.assert_not_called()
