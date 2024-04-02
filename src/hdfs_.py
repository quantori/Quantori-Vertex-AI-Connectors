from google.cloud import storage, dataproc_v1, compute_v1
from hdfs import InsecureClient
from utils.logging_util import get_logger
import hashlib
import datetime
import re

logger = get_logger(__name__)


#class Hdfs:
def get_instances_metadata(project_id: str, region: str, cluster_name: str) -> dict:
    """ Iterate through the instance metadata and returns external IP for existed instance.
        This function needs to be improved if there is more than one instance
    """

    dataproc_client = dataproc_v1.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    compute_client = compute_v1.InstancesClient()

    # Get cluster info
    cluster = dataproc_client.get_cluster(request={"project_id": project_id, "region": region, "cluster_name": cluster_name})
    zone = cluster.config.gce_cluster_config.zone_uri.split('/')[-1]

    instances_metadata = {}  # just one instance expected

    # Iterate over instances in the cluster
    for instance_group in [cluster.config.master_config, cluster.config.worker_config]:
        for instance in instance_group.instance_names:
            instance_details = compute_client.get(project=project_id, zone=zone, instance=instance)
            instance_name = instance_details.name
            # print(f'====== {instance} ======\n\n{instance_details}\n')

            instances_metadata["internal_ip"] = [interface.network_i_p for interface in instance_details.network_interfaces][0]

            # Find external IP
            for interface in instance_details.network_interfaces:
                for access_config in interface.access_configs:
                    if access_config.name == "External NAT":
                        instance_external_ip = access_config.nat_i_p
                        instances_metadata["instance_name"] = instance_name
                        instances_metadata["external_ip"] = instance_external_ip

    return instances_metadata


def get_mime_type(file_ext: str) -> str:
    if file_ext == 'pdf':
        return 'application/pdf'  # PDF, only native PDFs are supported
    elif file_ext == 'docx':
        return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    elif file_ext == 'pptx':
        return 'text/plain'


def get_hash(data: str, size: int = 32) -> str:
    key = hashlib.sha256()
    key.update(data.encode("utf-8"))
    return key.hexdigest()[:size]


def get_files_metadata(internal_ip, src_hdfs_dir, bucket_dir):
    hdfs_ip_port = f'{internal_ip}:9870'
    hdfs_client = InsecureClient(f'http://{hdfs_ip_port}')
    hdfs_folder_str = f'/{src_hdfs_dir}' if len(src_hdfs_dir) > 0 else '/'
    files_metadata = []

    try:
        files = hdfs_client.list(hdfs_folder_str, status=True)
    except Exception as e:
        logger.warning(f'Error listing directory {src_hdfs_dir}: {e}')
        return None

    for file_name, file_info in files:
        hdfs_file_uri = f'hdfs://{hdfs_ip_port}/{src_hdfs_dir}/{file_name}' if src_hdfs_dir else f'hdfs://{hdfs_ip_port}/{file_name}'
        blob_uri = f'{bucket_dir}/{file_name}'

        file_properties = {
            'name': file_info['pathSuffix'],
            'ext': file_info['pathSuffix'].split('.')[-1],
            'size': file_info['length'],
            'creation_time': datetime.datetime.fromtimestamp(file_info['modificationTime'] / 1000).isoformat(),
            'last_modified': datetime.datetime.fromtimestamp(file_info['modificationTime'] / 1000).isoformat(),
        }

        files_metadata.append({
            'id': get_hash(hdfs_file_uri),
            'struct_data': {
                'blob_path': blob_uri,
                'blob_properties': file_properties
            },
            'content': {
                'mime_type': get_mime_type(file_properties['ext']),
                'uri': blob_uri
            }
        })
    return files_metadata


def copy_files_from_hdfs_to_bucket(key, project_id, region, cluster_name, src_hdfs_dir, bucket_dir, internal_ip=None, filter=None) -> list[str]:
    """ Full load. All files will be rewritten
        In order to fetch internal IP automatically add role (todo check which one)
    """

    #instance_metadata = get_instances_metadata(project_id, region, cluster_name)
    #internal_ip = instance_metadata.get('internal_ip', None)
    instance_name = '' #instance_metadata.get('instance_name', None)

    if internal_ip is None:
        logger.warning(f'Unable to fetch internal IP of the instance {instance_name}')
        return

    hdfs_uri = f'http://{internal_ip}:9870'
    hdfs_client = InsecureClient(hdfs_uri)
    hdfs_folder_str = f'/{src_hdfs_dir}' if len(src_hdfs_dir) > 0 else '/'

    try: files = hdfs_client.list(hdfs_folder_str)
    except Exception as e:
        files = None
        logger.warning(f'Error listing directory {src_hdfs_dir}: {e}')
        return None

    if files is not None:
        if bucket_dir.startswith('gs://'):
            bucket_split_path = bucket_dir[len('gs://'):].split('/')
            bucket_name = bucket_split_path[0]
            base_path_in_bucket = '/'.join(bucket_split_path[1:])
        else:
            logger.warning(f'Incorrect path to the bucket directory: "{bucket_dir}"')
            return

        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)

        processed_files = []
        for file_name in files:
            if filter is not None and not re.match(filter, file_name):
                continue

            processed_files.append(file_name)
            hdfs_file_path = f'{hdfs_folder_str}/{file_name}'
            gcs_file_path = f'{base_path_in_bucket}/{file_name}'

            hdfs_client.resolve(hdfs_folder_str)

            with hdfs_client.read(hdfs_file_path) as reader:
                blob = bucket.blob(gcs_file_path)
                blob.upload_from_file(reader)

        logger.info(f"{len(processed_files)} files have been fetched from HDFS")
        return processed_files

