import sys
import uuid
from vertex_ai_search import VertexAISearch
from connector import Connector
from utils.logging_util import get_logger
from config import read_config
from gcp.secret import access_secret_version
from datetime import datetime, timezone
import os


logger = get_logger(__name__)


def main():
    config = read_config()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_id = timestamp + "_" + uuid.uuid4().hex
    logger.info(f"Run ID: {run_id}")
    gcp_staging_gcs_prefix = config.gcp_staging_gcs_prefix.strip("/")
    gcs_working_directory = gcp_staging_gcs_prefix + "/runs/" + run_id

    # secret key integration in process
    key = None  #access_secret_version(config.source.auth_config.key_secret)

    connector = Connector(key, config.source, config.state_location, gcs_working_directory)

    if config.export_method == "full":  # todo find out how to load files incrementally
        data_path = connector.full_export()
    else:
        raise ValueError(f"Invalid export method: {config.export_method}")

    if data_path is None:
        logger.warning("No data to import to Vertex AI Search")
        return

    vertex_loader = VertexAISearch(config.destination.project, config.destination.location)
    vertex_loader.process(gcs_working_directory, data_path, config.export_method, config.source.with_metadata, config.destination)
    logger.info("Successfully imported data to Vertex AI Search")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception("Failed process data, e=" + str(e))
        sys.exit(1)

