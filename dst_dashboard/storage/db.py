"""MongoDB database client for storing experiments and datasets."""

import json
import threading
from typing import Any, Dict, List, Optional

from gridfs import GridFSBucket, NoFile
from pymongo import MongoClient

from dst_dashboard.config.constants import Constants
from dst_dashboard.config.data_structures import DataSourceConfig

# Process-wide MongoClient. MongoClient manages its own internal connection
# pool and is thread-safe, so a single instance is created lazily on first use
# and reused by every DSTDatabase() instantiation for the life of the process
# instead of opening a new connection per call.
_client_lock = threading.Lock()
_client: Optional[MongoClient] = None
_indexes_ensured = False


def _get_client() -> MongoClient:
    """Get (creating if needed) the shared, process-wide MongoClient."""
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = MongoClient(str(Constants.DST_MONGO_URI))
    return _client


def _json_default(value: Any) -> str:
    """json.dumps fallback for datetime-like objects (e.g. pandas Timestamp).

    BSON serializes these natively via isinstance(value, datetime.datetime),
    but plain json.dumps doesn't - GridFS stores raw JSON bytes, so this
    fills that gap the same way FastAPI's own encoder would.
    """
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class DSTDatabase:
    """Database client for DST Dashboard, backed by MongoDB."""

    def __init__(self):
        """Initialize database client using the shared MongoClient."""
        self.client = _get_client()
        self.db = self.client[str(Constants.DST_MONGO_DB_NAME)]

        # Collections
        self.experiments = self.db.experiments
        self.datasources = self.db.datasources

        # Dataset rows and panel ECharts specs can exceed MongoDB's 16MB
        # document limit (millions of rows, or huge "top N" series arrays),
        # so both are stored via GridFS, which transparently chunks large
        # content and reassembles it on read.
        self.dataset_fs = GridFSBucket(self.db, bucket_name="datasets")
        self.panel_fs = GridFSBucket(self.db, bucket_name="panels")

        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create unique indexes once per process. Cheap no-op on subsequent calls."""
        global _indexes_ensured
        if _indexes_ensured:
            return
        with _client_lock:
            if _indexes_ensured:
                return
            self.experiments.create_index("id", unique=True)
            self.experiments.create_index("title", unique=True)
            self.datasources.create_index("name", unique=True)
            self.db["datasets.files"].create_index("metadata.experiment_id")
            self.db["panels.files"].create_index("metadata.experiment_id")
            _indexes_ensured = True

    def store_experiment(self, experiment: Dict[str, Any]) -> str:
        """Store experiment configuration. Returns the experiment ID."""
        self.experiments.update_one({"id": experiment["id"]}, {"$set": experiment}, upsert=True)
        return experiment["id"]

    def get_experiment(self, experiment_id: str) -> Optional[Dict[str, Any]]:
        """Get experiment by ID."""
        return self.experiments.find_one({"id": experiment_id}, {"_id": 0})

    def list_experiments(self) -> List[Dict[str, Any]]:
        """List all experiments."""
        return list(self.experiments.find({}, {"_id": 0}))

    def store_dataset(
        self, experiment_id: str, dataset_name: str, data: List[Dict[str, Any]]
    ) -> str:
        """Store dataset data via GridFS. Returns the dataset ID."""
        dataset_id = f"{experiment_id}:{dataset_name}"
        self._delete_gridfs_file(self.dataset_fs, dataset_id)
        self.dataset_fs.upload_from_stream(
            dataset_id,
            json.dumps(data, default=_json_default).encode("utf-8"),
            metadata={
                "experiment_id": experiment_id,
                "name": dataset_name,
                "row_count": len(data),
            },
        )
        return dataset_id

    def get_dataset(self, experiment_id: str, dataset_name: str) -> Optional[List[Dict[str, Any]]]:
        """Get dataset data."""
        dataset_id = f"{experiment_id}:{dataset_name}"
        payload = self._download_gridfs_file(self.dataset_fs, dataset_id)
        return json.loads(payload) if payload is not None else None

    def list_panels(self, experiment_id: str) -> List[Dict[str, Any]]:
        """List stored panel metadata for an experiment."""
        docs = self.db["panels.files"].find(
            {"metadata.experiment_id": experiment_id}, {"_id": 0, "filename": 1, "metadata": 1}
        )
        return [
            {
                "id": doc["filename"],
                "experiment_id": doc["metadata"]["experiment_id"],
                "name": doc["metadata"]["name"],
            }
            for doc in docs
        ]

    def store_panel_data(self, experiment_id: str, panel_name: str, data: Dict[str, Any]) -> str:
        """Store transformed panel data via GridFS. Returns the panel data ID."""
        panel_id = f"{experiment_id}:{panel_name}"
        self._delete_gridfs_file(self.panel_fs, panel_id)
        self.panel_fs.upload_from_stream(
            panel_id,
            json.dumps(data, default=_json_default).encode("utf-8"),
            metadata={"experiment_id": experiment_id, "name": panel_name},
        )
        return panel_id

    def get_panel_data(self, experiment_id: str, panel_name: str) -> Optional[Dict[str, Any]]:
        """Get transformed panel data."""
        panel_id = f"{experiment_id}:{panel_name}"
        payload = self._download_gridfs_file(self.panel_fs, panel_id)
        return json.loads(payload) if payload is not None else None

    def insert_datasource(self, datasource: DataSourceConfig) -> DataSourceConfig:
        """Store datasource configuration."""
        data = datasource.model_dump()
        self.datasources.update_one({"name": data["name"]}, {"$set": data}, upsert=True)
        return datasource

    def insert_datasource_list(self, datasources: List[DataSourceConfig]) -> List[DataSourceConfig]:
        """Store a list of datasource configurations."""
        for datasource in datasources:
            self.insert_datasource(datasource)
        return datasources

    def dataset_exists(self, experiment_id: str, dataset_name: str) -> bool:
        """Check if dataset exists in database."""
        dataset_id = f"{experiment_id}:{dataset_name}"
        return self.db["datasets.files"].find_one({"filename": dataset_id}, {"_id": 1}) is not None

    def get_dataset_metadata(
        self, experiment_id: str, dataset_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get dataset metadata without data rows."""
        dataset_id = f"{experiment_id}:{dataset_name}"
        doc = self.db["datasets.files"].find_one(
            {"filename": dataset_id}, {"_id": 0, "filename": 1, "metadata": 1}
        )
        if doc is None:
            return None
        return {
            "id": doc["filename"],
            "experiment_id": doc["metadata"]["experiment_id"],
            "name": doc["metadata"]["name"],
            "row_count": doc["metadata"]["row_count"],
        }

    def delete_experiment(self, experiment_id: str) -> bool:
        """Delete an experiment and cascade to its datasets and panels."""
        result = self.experiments.delete_one({"id": experiment_id})
        self._delete_gridfs_files_matching(
            self.dataset_fs, "datasets", {"metadata.experiment_id": experiment_id}
        )
        self._delete_gridfs_files_matching(
            self.panel_fs, "panels", {"metadata.experiment_id": experiment_id}
        )
        return result.deleted_count > 0

    def delete_dataset(self, experiment_id: str, dataset_name: str) -> bool:
        """Delete a dataset."""
        dataset_id = f"{experiment_id}:{dataset_name}"
        return self._delete_gridfs_file(self.dataset_fs, dataset_id)

    def delete_panel(self, experiment_id: str, panel_name: str) -> bool:
        """Delete a panel."""
        panel_id = f"{experiment_id}:{panel_name}"
        return self._delete_gridfs_file(self.panel_fs, panel_id)

    @staticmethod
    def _delete_gridfs_file(bucket: GridFSBucket, filename: str) -> bool:
        """Delete a GridFS file by filename. Returns True if a file was found and deleted."""
        deleted = False
        for file_doc in bucket.find({"filename": filename}):
            bucket.delete(file_doc._id)
            deleted = True
        return deleted

    def _delete_gridfs_files_matching(
        self, bucket: GridFSBucket, bucket_name: str, query: Dict[str, Any]
    ) -> None:
        """Delete every GridFS file in a bucket matching a metadata query."""
        file_ids = [doc["_id"] for doc in self.db[f"{bucket_name}.files"].find(query, {"_id": 1})]
        for file_id in file_ids:
            bucket.delete(file_id)

    @staticmethod
    def _download_gridfs_file(bucket: GridFSBucket, filename: str) -> Optional[bytes]:
        """Download a GridFS file's raw bytes by filename, or None if it doesn't exist."""
        try:
            return bucket.open_download_stream_by_name(filename).read()
        except NoFile:
            return None
