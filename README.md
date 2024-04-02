# Setup Hadoop Dataproc to Google Vertex AI Connector

## Introduction
This documentation outlines the steps required to deploy the Hadoop to Vertex AI connector which is designed to transfer files from Hadoop running on Dataproc to Vertex AI to perform full-text search across the files


## Setting Up the GCP Environment 

#### 1. Create a new project in the Google Cloud Console. 
#### 2. Enable APIs:
Ensure that necessary APIs are enabled for your project.
- Compute Engine API
- Cloud Dataproc API
- Cloud Resource Manager API 
- Cloud build API


#### 3. Granting necessary roles: 
- Dataproc Viewer (roles/dataproc.viewer)
- Compute Viewer (roles/compute.viewer).

Replace [SERVICE-ACCOUNT-EMAIL] with your service account email 
and [PROJECT-ID] with your project ID and run those 2 commands in terminal

```
gcloud projects add-iam-policy-binding [PROJECT-ID] \
  --member="serviceAccount:[SERVICE-ACCOUNT-EMAIL]" \
  --role="roles/dataproc.worker"
```
```
gcloud projects add-iam-policy-binding [PROJECT-ID] \
  --member="serviceAccount:[SERVICE-ACCOUNT-EMAIL]" \
  --role="roles/compute.viewer"
```

#### 4. Setup a VPC Network: 
- Go to https://console.cloud.google.com/networking/networks
- Click on "Create VPC network".
- Provide a name for your VPC network. The name must be unique within the project.
- Set Subnet Creation Mode to custom mode: It allows you to manually specify the subnets and their IP ranges.
- Provide a name for the subnet.
- Select the region for the subnet. It's a good practice to create subnets in regions where you'll deploy your resources.
- Set IP stack type to IPv4 (single-stack)
- IP Address range: Specify the CIDR block. Make sure it doesn't overlap with other subnets in your VPC or with your local network if you plan to set up a VPN.
- Set Private Google Access to On
- Click Done
- Click Create
- Ensure that the creation has been finished successfully


#### 5. Configure Firewall Rules
- Go to https://console.cloud.google.com/net-security/firewall-manager 
- Click on "Create Firewall Rule".
- Specify the name, targets
- Choose the network that you have created at the 3rd step
- Set Source filter to IPv4 ranges and set IP address range that you have added in 3rd step in VPC network
- Set Protocols and ports to Specified protocols and ports 
- Input TCP Ports: 9870, 9864
- Click Create
- Ensure that the creation has been finished successfully


#### 6. Create the Dataproc Cluster
- Go to https://console.cloud.google.com/dataproc/clusters 
- Click create new cluster -> Cluster on Compute Engine
###### Set up cluster section
- Specify name of a cluster
- Set your region (the same as in VPC)
- Set zone to Any
- Set cluster type Single Node (1 master, 0 workers)
- Configure the cluster to operate within your previously created VPC network. At the Network Configuration set Networks in this project and choose Primary network and Subnetwork
- At Components set Enable component gateway
- At optional components set Jupyter notebook
###### Configure nodes section
- Set custom parameters
###### Customize cluster section
- At Internal IP only section, set Configure all instances to have only internal IP addresses.
###### Manage security
- pass

Also you can create a cluster using the gcloud command below, insert your parameters before running
```
gcloud dataproc clusters create [CLUSTER-NAME] \
--enable-component-gateway \
--region [YOUR-REGION]\
--subnet [YOUR-VPC-SUBNET-NAME] \
--no-address \
--single-node \
--master-machine-type n2-standard-4 \
--master-boot-disk-type pd-balanced \
--master-boot-disk-size [CAPACITY-GIGABYTES] \
--image-version 2.2-debian12 \
--optional-components JUPYTER \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--project [PROJECT-ID]
```

#### 7. Setup a Google Cloud Storage
- Go to https://console.cloud.google.com/storage/browser 
- Create a new bucket in Cloud Storage. Remember the bucket name. This bucket will serve as the storage location to save metadata of every run of transferred files from HDFS to Vertex AI.
- Create folders in this bucket **jobs/hadoop-to-vertex-connector**


#### 8. Upload files to HDFS
- Create a Cloud Storage Bucket: 
- This bucket will store the files you intend to upload to HDFS, as well as any associated metadata. You can create a bucket through the Cloud Storage browser in the Google Cloud Console.

###### Upload Your Files:
- Create a folder within your bucket.
- Upload the files you wish to transfer to HDFS into this folder.

###### Create a Manifest File: 
- It should be a txt file (ex. manifest.txt). This file should list the paths of all files in the bucket that you plan to upload to HDFS. The paths should be relative to the root of the Cloud Storage bucket.

###### Access JupyterLab via Dataproc
- Navigate to Your Dataproc Clusters https://console.cloud.google.com/dataproc/clusters
- Ensure that the cluster you intend to use is in the "Running" state and note its name.
- Click on the name of your cluster to view its details.
- Find the "Web Interfaces" section.
- Click on the "JupyterLab" link to open JupyterLab through the Component Gateway.  
Alternatively, you can find the direct link to JupyterLab under the "Equivalent REST" section of the cluster details page. The link looks like this:
```
{
  "config": {
    "endpointConfig": {
      "httpPorts": {
        "JupyterLab": "https://link-to-jupiterlab-web-interface"
      }
    }
  }
}
```

###### Run the code in JupiterLab
- In the JupyterLab interface, select the option to create a new Python notebook.
- Insert the Python code provided below into your new notebook. This code will handle the transfer of files from Cloud Storage to HDFS. Be sure to adjust any global variables in the script to match your specific configuration, such as file paths and cluster details.
- Run the notebook to execute your script. This will start the process of transferring your files from Cloud Storage to HDFS.
```
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import Job, HadoopJob
import time

PROJECT_ID = [YOUR-PROJECT]
REGION = [YOUR-REGION]
CLUSTER_NAME = [YOUR-DATAPROC-CLUSTER]
BUCKET = [YOUR-BUCKET]
MANIFEST = '[MANIFEST-FILE-NAME].txt'
SRC_BUCKET_FOLDER = [SOURCE-FOLDER-IN-YOUR-BUCKET]
DST_HDFS_FOLDER = [DESTINATION-FOLDER-IN-HDFS]


""" If file exists in HDFS it won't be rewritten.  """
job_client = dataproc_v1.JobControllerClient(client_options={'api_endpoint': f'{REGION}-dataproc.googleapis.com:443'})

hdfs_full_path = f'hdfs:///{DST_HDFS_FOLDER}/'
hadoop_job = HadoopJob(
    main_class='org.apache.hadoop.tools.DistCp',
    args=['-f', f'gs://{BUCKET}/{MANIFEST}', f'hdfs:///{DST_HDFS_FOLDER}/']
)

job = Job(
    placement={'cluster_name': CLUSTER_NAME},
    hadoop_job=hadoop_job,
)

result = job_client.submit_job(project_id=PROJECT_ID, region=REGION, job=job)

job_id = result.reference.job_id
job_status = {'status': '', 'details': ''}

print(f'Job ID:"{job_id}" has been submitted to copy files from bucket "{SRC_BUCKET_FOLDER}" to HDFS "{DST_HDFS_FOLDER}"')
while True:
    job_state = job_client.get_job(project_id=PROJECT_ID, region=REGION, job_id=job_id)
    status = job_state.status.state.name
    job_start_time = job_state.status.state_start_time
    if status in ['ERROR', 'DONE', 'CANCELLED']:
        print(f'Job ID:"{job_id}" finished with status: {status}')
        job_status["status"] = status
        job_status["details"] = job_state.status.details
        if status == 'ERROR':
            print(job_status["details"])
        break
    time.sleep(1)

job_status
```

#### 8. Setup the input.json file
- Come up with the connector name and connector ID
- Go to **src/configs** and open input.json
- Create the **input.json** file as suggested below and set the parameters in it
- Upload this file **input.json** into the root of your bucket
```
{
   "connector_name": [CONNECTOR-NAME],
   "connector_id": [CONNECTOR-ID],


   "source": {
       "auth_config": {
           "key_secret": "none"
       },
       "project": [YOUR-PROJECT],
       "cluster_name": [YOUR-CLUSTER-NAME],
       "region": [YOUR-REGION-NAME],
       "internal_ip": [PRIMARY-INTERNAL-IP-ADDRESS],
       "with_metadata": true,
       "prefix": [SOURCE-HDFS-FOLDER],
       "name_regex": null
   },

   "export_method": full,
   "gcp_staging_gcs_prefix": "gs://[BUCKET-NAME]/jobs/hadoop-to-vertex-connector/",
   "state_location": "gs://[BUCKET-NAME]/jobs/hadoop-to-vertex-connector/state.json",

   "destination": {
       "project": [YOUR-PROJECT],
       "location": "global",
       "data_store_id": null,
       "data_store_display_name": [DESTINATION-DATA-STORE-IN-VERTEX-AI],
       "with_content": true,
       "allow_create_data_store": true,
       "search_first": true
   }
}
```
- You can find your Primary internal IP address here: 
go to **YOUR CLUSTER -> VM INSTANCES -> MASTER INSTANCE -> Network interfaces**


#### 9. Vertex AI (activation)
- Go to https://console.cloud.google.com/gen-app-builder/start 
- Activate API as suggested


#### 10. Create a Cloud run job
- Get the URL of your Docker image that you purchased on Google Cloud Marketplace 
- Execute the gcloud command below You have two options to do it:

###### Execute from the terminal on your computer 
Make sure you have Google Cloud SDK (gcloud) installed and configured so that the command is available.
###### Execute from the Cloud Shell in the GCP console, 
Click on the "Activate Cloud Shell" button in the upper right corner of the console.
```
gcloud run jobs deploy [CLOUD-RUN-JOB-NAME] \
   --image [IMAGE-URL] \
   --tasks 1 \
   --set-env-vars INPUT_FILE="gs://[BUCKET-NAME]/input.json" \
   --set-env-vars LOG_LEVEL="DEBUG" \
   --set-env-vars LOG_FORMAT="CLOUD" \
   --max-retries 0 \
   --region [YOUR-REGION] \
   --project [YOUR-PROJECT]
```
Replace [IMAGE-URL] with the URL of your Docker image that you purchased on Google Cloud Marketplace.
- Run the job at cloud run https://console.cloud.google.com/run/jobs 


#### 11. Vertex AI (create an app)
- Go to https://console.cloud.google.com/gen-app-builder/start 
- At Apps section click New app
- Enter app name and company name
- Choose the data store that was created after running the job ab cloud run.  
The name of the data store is configured in
**input.json -> destination -> data_store_display_name**

