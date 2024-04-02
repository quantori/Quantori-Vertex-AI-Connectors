import json
import re

from google.api_core import page_iterator
from google.cloud import storage
from google.cloud.storage.fileio import BlobWriter


def _parse_gcs_path(gcs_path):
    match = re.match(r'gs://([^/]+)(/.*$)?', gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    bucket_name = match.group(1)
    prefix = match.group(2).lstrip('/') if match.group(2) else ''
    return bucket_name, prefix


def _get_blob(gcs_path):
    bucket_name, blob_name = _parse_gcs_path(gcs_path)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob


def save_to_gcs_as_json(gcs_path, data, indent=None):
    blob = _get_blob(gcs_path)
    json_data = json.dumps(data, indent=indent)
    blob.upload_from_string(json_data, content_type='application/json')


def save_to_gcs_as_string(gcs_path, data, content_type='text/plain'):
    blob = _get_blob(gcs_path)
    blob.upload_from_string(data, content_type=content_type)


def save_to_gcs_as_ndjson(gcs_path, data):
    blob = _get_blob(gcs_path)
    with BlobWriter(blob, content_type='application/x-ndjson') as writer:
        for line in data:
            line_json = json.dumps(line) + "\n"
            writer.write(line_json.encode("utf-8"))


def save_to_gcs(gcs_path, local_path):
    blob = _get_blob(gcs_path)
    blob.upload_from_filename(local_path)


def exists(gcs_path):
    blob = _get_blob(gcs_path)
    return blob.exists()


def read_from_gcs_as_json(gcs_path):
    blob = _get_blob(gcs_path)
    if not blob.exists():
        raise ValueError(f"Blob not found: {gcs_path}")
    json_data = blob.download_as_text()
    return json.loads(json_data)


def read_from_gcs_as_text(gcs_path):
    blob = _get_blob(gcs_path)
    if not blob.exists():
        raise ValueError(f"Blob not found: {gcs_path}")
    return blob.download_as_text()


def list_files_gcs(gcs_path, max_results=None):
    bucket_name, prefix = _parse_gcs_path(gcs_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if prefix != '' and not prefix.endswith('/'):
        prefix += '/'
    iterator = bucket.list_blobs(prefix=prefix, delimiter='/', max_results=max_results)
    return iterator


# https://github.com/googleapis/python-storage/issues/294
def list_prefixes_gcs(gcs_path, max_results=None):
    bucket_name, prefix = _parse_gcs_path(gcs_path)
    storage_client = storage.Client()
    return page_iterator.HTTPIterator(
        client=storage_client,
        api_request=storage_client._connection.api_request,
        path=f"/b/{bucket_name}/o",
        items_key="prefixes",
        max_results=max_results,
        item_to_value=lambda iterator, item: item,
        extra_params={
            "projection": "noAcl",
            "prefix": prefix,
            "delimiter": "/",
        },
    )