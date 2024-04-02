import json
import os

from pydantic import BaseModel
from typing import Optional

from gcp.gcs_util import read_from_gcs_as_json


#class AuthConfig(BaseModel):
#    key_secret: str


class SourceConfig(BaseModel):
    #auth_config: AuthConfig
    project: str
    cluster_name: str
    region: str
    internal_ip: str
    with_metadata: Optional[bool] = False
    prefix: str
    name_regex: Optional[str] = None


class DestinationConfig(BaseModel):
    project: str
    location: str
    data_store_id: Optional[str] = None
    data_store_display_name: Optional[str] = None
    with_content: Optional[bool] = False
    allow_create_data_store: bool = False
    search_first: bool = False


class ConnectorConfig(BaseModel):
    connector_name: str
    connector_id: str
    source: SourceConfig
    export_method: str
    gcp_staging_gcs_prefix: str
    state_location: Optional[str] = None
    destination: DestinationConfig


def read_config() -> ConnectorConfig:
    config_path = os.environ.get("INPUT_FILE")
    if not config_path:
        raise ValueError("Config file not found: INPUT_FILE environment variable not set")
    if config_path.startswith("gs://"):
        conf_dict = read_from_gcs_as_json(config_path)
    elif os.path.exists(config_path):
        with open(config_path, "r") as f:
            conf_dict = json.load(f)
    else:
        raise ValueError(f"Config file not found: {config_path}")
    return ConnectorConfig(**conf_dict)

