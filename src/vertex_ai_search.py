import time
from typing import Optional, Generator

from google.api_core.exceptions import GoogleAPICallError, NotFound, FailedPrecondition
from google.cloud.discoveryengine_v1alpha import *
from utils.logging_util import get_logger


logger = get_logger(__name__)


def handle_operation(operation):
    try:
        operation.result()
    except GoogleAPICallError as e:
        if "Long-running operation had neither response nor error set" in e.message:
            pass
        else:
            raise e


class VertexAISearch:
    def __init__(self, project, location, collection="default_collection", branch="default_branch", industry_vertical=IndustryVertical.GENERIC):
        self.project = project
        self.location = location
        self.collection = collection
        self.branch = branch
        self.industry_vertical = industry_vertical
        self.schema_service_client = None
        self.data_store_service_client = None
        self.document_service_client = None

    def get_schema_service_client(self) -> SchemaServiceClient:
        if self.schema_service_client is None:
            self.schema_service_client = SchemaServiceClient()
        return self.schema_service_client

    def get_data_store_service_client(self) -> DataStoreServiceClient:
        if self.data_store_service_client is None:
            self.data_store_service_client = DataStoreServiceClient()
        return self.data_store_service_client

    def get_document_service_client(self) -> DocumentServiceClient:
        if self.document_service_client is None:
            self.document_service_client = DocumentServiceClient()
        return self.document_service_client

    def check_schema(self, data_store_id, schema_id) -> Optional[Schema]:
        request = GetSchemaRequest(
            name=f"projects/{self.project}/locations/{self.location}/collections/{self.collection}/dataStores/{data_store_id}/schemas/{schema_id}"
        )
        try:
            schema = self.get_schema_service_client().get_schema(request)
        except NotFound:
            schema = None
        return schema

    def check_datas_store(self, data_store_id) -> Optional[DataStore]:
        request = GetDataStoreRequest(
            name=f"projects/{self.project}/locations/{self.location}/collections/{self.collection}/dataStores/{data_store_id}",
        )
        try:
            data_store = self.get_data_store_service_client().get_data_store(request=request)
        except NotFound:
            data_store = None
        return data_store

    def delete_documents(self, name):

        operation = self.get_document_service_client().delete_document()
        handle_operation(operation)
        logger.warning(f"Documents deleted: {name}")

    def purge_data_data_store(self, name):
        request = PurgeDocumentsRequest(
            parent=f"{name}/branches/{self.branch}",
            filter="*",
            force=True,
        )
        operation = self.get_document_service_client().purge_documents(request)
        response = operation.result()
        logger.warning(f"Data purged: {name}, count={response.purge_count}")

    def delete_data_store(self, name, purge):
        if purge:
            self.purge_data_data_store(name)
        request = DeleteDataStoreRequest(
            name=name
        )
        try:
            operation = self.get_data_store_service_client().delete_data_store(request)
        except FailedPrecondition as e:
            logger.warning(f"Failed to delete data store: {name}, e={e}")
            return

        handle_operation(operation)
        logger.warning(f"Data store deleted: {name}")

    def list_datas_stores(self, prefix="") -> Generator[DataStore, None, None]:
        request = ListDataStoresRequest(
            parent=f"projects/{self.project}/locations/{self.location}/collections/{self.collection}",
        )
        list_data_stores_pager = self.get_data_store_service_client().list_data_stores(request)
        for data_store in list_data_stores_pager:
            if data_store.display_name.startswith(prefix):
                yield data_store

    def delete_by_prefix(self, prefix, purge):
        data_stores = list(self.list_datas_stores(prefix))
        logger.warning(f"Deleting data stores: {len(data_stores)}")
        for data_store in data_stores:
            self.delete_data_store(data_store.name, purge)


    # {"displayName":"test-3","industryVertical":"GENERIC","contentConfig":"CONTENT_REQUIRED",
    # "solutionTypes":[],
    # "searchTier":"STANDARD",
    # "searchAddOns":[],
    # "aclEnabled":false,
    # "documentProcessingConfig":{"defaultParsingConfig":{"ocrParsingConfig":{"enhancedDocumentElements":["table"],"useNativeText":true}}}}# todo
    def create_data_store(self, data_store_display_name, data_store_id, with_content=False) -> Optional[DataStore]:
        if with_content:
            content_config = DataStore.ContentConfig.CONTENT_REQUIRED
        else:
            content_config = DataStore.ContentConfig.NO_CONTENT

        data_store = DataStore(
            display_name=data_store_display_name,
            industry_vertical=self.industry_vertical,
            content_config=content_config
        )
        if data_store_id is None:
            data_store_id = f"{data_store_display_name}_{int(time.time()*1000)}"
        request = CreateDataStoreRequest(
            parent=f"projects/{self.project}/locations/{self.location}/collections/{self.collection}",
            data_store=data_store,
            data_store_id=data_store_id,
        )
        operation = self.get_data_store_service_client().create_data_store(request=request)
        handle_operation(operation)
        return self.check_datas_store(data_store_id)

    # {"gcsSource":{"inputUris":["gs://azure-data-lake-connector-staging/jobs/azure-data-lake-to-vertex-connector-test/20240307_144743_86bde1623b2d419c98b4b3e1ce7160d7/metadata.json"],
    # "dataSchema":"document"},"reconciliationMode":"FULL","autoGenerateIds":false}
    def import_data(self, full_data_store_name, input_uris, error_prefix, is_full, data_schema, auto_generate_ids, id_field) -> ImportDocumentsResponse:
        gcs = GcsSource(input_uris=input_uris, data_schema=data_schema)

        if is_full:
            reconciliation_mode = ImportDocumentsRequest.ReconciliationMode.FULL
        else:
            reconciliation_mode = ImportDocumentsRequest.ReconciliationMode.INCREMENTAL

        request = ImportDocumentsRequest(
            gcs_source=gcs,
            parent=f"{full_data_store_name}/branches/{self.branch}",
            reconciliation_mode=reconciliation_mode,
            auto_generate_ids=auto_generate_ids,
            id_field=id_field,
            error_config=ImportErrorConfig(gcs_prefix=error_prefix)
        )

        response = self.get_document_service_client().import_documents(request)
        return response.result()

    def process(self, gcs_working_directory, data_path, export_method, with_metadata, destination_config):
        if export_method not in ["full", "incremental"]:
            raise ValueError(f"Unsupported export method: {export_method}")

        if destination_config.data_store_id is not None:
            data_store = self.check_datas_store(destination_config.data_store_id)
            if data_store:
                logger.warning(f"Data store found: {data_store.name}")
        else:
            data_store = None

        if data_store is None:
            assert destination_config.data_store_display_name is not None, "Data store display name is required if data_store_id is not provided"
            if destination_config.search_first:
                try:
                    data_store = next(self.list_datas_stores(prefix=destination_config.data_store_display_name))
                    logger.info(f"Data store found: {data_store.name} by prefix")
                except StopIteration:
                    pass

        if data_store is None:
            if destination_config.allow_create_data_store:
                data_store = self.create_data_store(destination_config.data_store_display_name, destination_config.data_store_id, destination_config.with_content)
                if data_store is None:
                    raise ValueError(f"Data store not found: {destination_config.data_store_id}, and creation failed")
                logger.warning(f"Created data store: {data_store.name}")
            else:
                raise ValueError(f"Data store not found: {destination_config.data_store_id}, and creation is not allowed")

        input_uris = [data_path]
        error_prefix = gcs_working_directory + "/errors/"

        is_full = export_method == "full"

        # google.api_core.exceptions.InvalidArgument: 400 (Field "auto_generate_ids" can only be set when the source is "GcsSource" or "BigQuerySource",
        # and when the GcsSource.data_schema or BigQuerySource.data_schema is `custom`, `csv` or `content-with-faq-csv`. Field "id_field" has the same
        # source and schemarequirements as "auto_generate_ids", and "id_field" can only be set when "auto_generate_ids" is unset or set as `false`.)
        if with_metadata:
            data_schema = "document"
            auto_generate_ids = None
            id_field = None
        else:
            data_schema = "custom"
            auto_generate_ids = False
            id_field = "id"

        logger.info(f"Importing data from to {data_store.name}")
        res = self.import_data(
            full_data_store_name=data_store.name,
            input_uris=input_uris,
            error_prefix=error_prefix,
            is_full=is_full,
            data_schema=data_schema,
            auto_generate_ids=auto_generate_ids,
            id_field=id_field)

        if res.error_samples and len(res.error_samples):
            logger.error(f"Error samples: {res.error_samples}")
            raise ValueError(f"Failed to import data, see errors: {res.error_config.gcs_prefix}")