import hashlib
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel

from config import SourceConfig
from gcp import gcs_util
from utils.logging_util import get_logger
from hdfs_ import copy_files_from_hdfs_to_bucket, get_files_metadata


logger = get_logger(__name__)


class State(BaseModel):
    state_timestamp: Optional[datetime]
    #last_modified: Optional[datetime]
    files_exported: int

    class Config:
        json_encoders = {
            datetime: lambda v: v.astimezone(tz=timezone.utc).isoformat().replace("+00:00", "Z")
        }


class Connector:
    def __init__(self, key, source_config: SourceConfig, state_location: Optional[str], gcs_working_directory: str):
        self.key = key
        self.source_config = source_config
        self.state_location = state_location
        self.gcs_working_directory = gcs_working_directory
        if state_location and gcs_util.exists(state_location):
            state_json = gcs_util.read_from_gcs_as_json(state_location)
            self.state = State(**state_json)
            logger.info(f"State loaded: {self.state}")
        else:
            self.state = State(state_timestamp=None, last_modified=None, files_exported=0)
            logger.warning(f"No state loaded: {self.state}")

    def update_state(self, last_modified, files_exported):
        if not self.state_location:
            return
        new_state = State(
            state_timestamp=datetime.now(timezone.utc),
            last_modified=last_modified if last_modified else self.state.last_modified,
            files_exported=files_exported
        )
        obj = new_state.model_dump_json(indent=4)
        gcs_util.save_to_gcs_as_string(self.state_location, obj, content_type='application/json')
        job_run_state_location = self.gcs_working_directory + "/" + "state.json"
        gcs_util.save_to_gcs_as_string(job_run_state_location, obj, content_type='application/json')

    def export(self, filter_re):
        start_timestamp = datetime.now(timezone.utc).isoformat()
        filter_regexp = self.source_config.name_regex if filter_re is not None else None  # todo check how the filter works
        secret_key = self.key
        project = self.source_config.project
        region = self.source_config.region
        cluster_name = self.source_config.cluster_name
        source_prefix = self.source_config.prefix
        data_path = self.gcs_working_directory + "/" + "data"
        internal_ip = self.source_config.internal_ip

        # todo fetch metadata here instead of files_matadata
        processed_files = copy_files_from_hdfs_to_bucket(secret_key,project,region,cluster_name,source_prefix,data_path,internal_ip,filter_regexp)

        files_metadata = get_files_metadata(internal_ip, source_prefix, data_path)
        if processed_files is not None:

            self.update_state(
                last_modified=max([lm["struct_data"]["blob_properties"]["last_modified"] for lm in files_metadata]),
                files_exported=len(processed_files)
            )

            if self.source_config.with_metadata:
                end_timestamp = datetime.now(timezone.utc).isoformat()
                meta = []
                metadata_path = self.gcs_working_directory + "/" + "metadata.json"
                for file in files_metadata:
                    meta.append({
                        "id": file["id"],
                        "structData": {
                            "blob_url": file["struct_data"]["blob_path"],
                            "export_start_timestamp": start_timestamp,
                            "export_end_timestamp": end_timestamp,
                            "blob_properties": file["struct_data"]["blob_properties"]
                        },
                        "content": {
                            "mimeType": file["content"]["mime_type"],
                            "uri": file["struct_data"]["blob_path"]
                        }
                    })
                gcs_util.save_to_gcs_as_ndjson(metadata_path, meta)
                return metadata_path
            else:
                return data_path + "/*"

    def full_export(self):
        return self.export(filter_re=None)
