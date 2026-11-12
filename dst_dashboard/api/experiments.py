from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from dst_dashboard.api.utils import get_processor
from dst_dashboard.auth import require_admin_token
from dst_dashboard.config.data_structures import ExperimentConfig
from dst_dashboard.storage.db import DSTDatabase

router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.get("/families")
def list_families(request: Request) -> Dict[str, Any]:
    """
    Get all experiment families with their experiments.
    Organized for navigation in the dashboard UI.

    Returns:
        List of families with nested experiments
    """
    db = DSTDatabase()

    # Get experiments from database (source of truth)
    experiments_data = db.list_experiments()
    experiments = [ExperimentConfig(**exp_data) for exp_data in experiments_data]

    # Group experiments by family
    families_dict = {}
    for experiment in experiments:
        if not experiment.publish:
            continue

        family = experiment.family
        if family not in families_dict:
            families_dict[family] = {"name": family, "experiments": []}

        families_dict[family]["experiments"].append(
            {
                "id": experiment.id,
                "title": experiment.title,
                "panel_count": len(experiment.panels),
                "dataset_count": len(experiment.datasets),
                "metadata": experiment.metadata,
                "description": experiment.description,
                "github_repo": experiment.github_repo,
                "github_pr": experiment.github_pr,
                "docker_image": experiment.docker_image,
                "date": experiment.date,
            }
        )

    # Convert to sorted list
    families = sorted(families_dict.values(), key=lambda x: x["name"])

    return {
        "families": families,
        "total_families": len(families),
        "total_experiments": sum(len(f["experiments"]) for f in families),
    }


@router.get("", response_model=List[ExperimentConfig])
def list_experiments(
    request: Request,
    publish: Optional[bool] = Query(None, description="Filter by publish status"),
):
    """List all experiments from database. Use ?publish=true to filter only published experiments."""
    db = DSTDatabase()

    # Get experiments from database (source of truth)
    experiments_data = db.list_experiments()
    experiments = [ExperimentConfig(**exp_data) for exp_data in experiments_data]

    if publish is not None:
        experiments = [experiment for experiment in experiments if experiment.publish == publish]

    return experiments


@router.get("/{experiment_id}", response_model=ExperimentConfig)
def get_experiment(experiment_id: str, request: Request):
    """Get experiment details from database."""
    db = DSTDatabase()

    # Get from database (source of truth)
    experiment_data = db.get_experiment(experiment_id)

    if experiment_data is None:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return ExperimentConfig(**experiment_data)


@router.post("", response_model=ExperimentConfig, status_code=201)
def create_experiment(
    experiment: ExperimentConfig, request: Request, _: None = Depends(require_admin_token)
):
    """
    Create a new experiment and process it (fetch datasets, generate panels).
    `id` is always server-assigned - any id in the request body is ignored.
    """
    db = DSTDatabase()

    # id is never client-supplied - always assign a fresh one
    experiment.id = str(ObjectId())

    # Title must be unique
    if db.experiments.find_one({"title": experiment.title}):
        raise HTTPException(
            status_code=409, detail=f"Experiment with title '{experiment.title}' already exists"
        )

    # Store experiment in database
    db.store_experiment(experiment.model_dump())

    # Process the experiment (fetch datasets, generate panels)
    processor = get_processor(request)
    try:
        processor.process_experiment(experiment)
        empty = [
            d.name
            for d in experiment.datasets
            if not (db.get_dataset_metadata(experiment.id, d.name) or {}).get("row_count")
        ]
        if empty:
            raise HTTPException(
                status_code=502, detail=f"No rows fetched for datasets {empty}; nothing was stored"
            )
    except Exception:
        db.delete_experiment(experiment.id)
        raise

    return experiment


@router.put("/{experiment_id}", response_model=ExperimentConfig)
def update_experiment(
    experiment_id: str,
    experiment: ExperimentConfig,
    request: Request,
    _: None = Depends(require_admin_token),
):
    """
    Update an existing experiment.
    Only reprocesses if configuration changed (datasets, panels, datasources, time ranges).
    `id` always comes from the URL path - any id in the request body is ignored.
    """
    db = DSTDatabase()

    # id is never client-supplied - the path is authoritative
    experiment.id = experiment_id

    # Get existing experiment from DB
    existing_data = db.get_experiment(experiment_id)
    if not existing_data:
        raise HTTPException(status_code=404, detail="Experiment not found")

    existing = ExperimentConfig(**existing_data)

    # Title must be unique among *other* experiments
    title_conflict = db.experiments.find_one(
        {"title": experiment.title, "id": {"$ne": experiment_id}}
    )
    if title_conflict:
        raise HTTPException(
            status_code=409, detail=f"Experiment with title '{experiment.title}' already exists"
        )

    # Check if reprocessing is needed
    needs_reprocessing = (
        existing.datasets != experiment.datasets
        or existing.panels != experiment.panels
        or any(
            existing_ds.datasource != new_ds.datasource or existing_ds.timeRange != new_ds.timeRange
            for existing_ds, new_ds in zip(existing.datasets, experiment.datasets)
        )
        if len(existing.datasets) == len(experiment.datasets)
        else True
    )

    # Store updated experiment in database
    db.store_experiment(experiment.model_dump())

    # Reprocess if configuration changed
    if needs_reprocessing:
        processor = get_processor(request)
        success = processor.process_experiment(experiment)

        if not success:
            # Rollback - restore old experiment
            db.store_experiment(existing.model_dump())
            raise HTTPException(
                status_code=500,
                detail="Failed to reprocess experiment. Rolled back to previous configuration.",
            )

    return experiment


@router.delete("/{experiment_id}", status_code=204)
def delete_experiment(experiment_id: str, request: Request, _: None = Depends(require_admin_token)):
    """Delete an experiment and all its associated data (datasets, panels)."""
    db = DSTDatabase()

    # Check if experiment exists
    if not db.get_experiment(experiment_id):
        raise HTTPException(status_code=404, detail="Experiment not found")

    # Delete from database (cascades to datasets and panels)
    db.delete_experiment(experiment_id)

    return None
