# Command Line Interfaces

*   [Global Setup](#global-setup)
*   [FeatureStore Load Test Framework](#featurestore-load-test-framework)
*   [Import Tool](#import-tool)
*   [Export Tool](#export-tool)

## Global Setup

For all the tools, you will need the
[gcloud CLI](https://cloud.google.com/sdk/docs/install).

Also the application default credentials will need to be configured:

```sh
gcloud auth application-default login
```

Also set your default project:

```sh
gcloud config set project your_project_name
```

## FeatureStore Load Test Framework

### Dependencies

*   [Python 3](https://www.python.org/downloads)
*   [Abseil](https://abseil.io/docs/python/quickstart)

Local:

*   [JRE](https://www.java.com/en/download/manual.jsp)
*   [Google Cloud Java Client](https://cloud.google.com/java/docs/reference/google-cloud-shell/latest)
    *   google-cloud-aiplatform

GKE:

*   [Kubernetes Python Client](https://github.com/kubernetes-client/python)
*   [Google API Client](https://github.com/googleapis/google-api-python-client)

### Function

The FeatureStore Load Test Framework:

*   Provisions a GKE cluster
*   Creates a job to run the load tester on each pod in the cluster
*   Deletes the cluster

### Use

#### Create python env and install dependencies

```bash
python3 -m venv env
. env/bin/activate
pip install -r requirements.txt
```

#### Local

To run the FeatureStore load test framework locally:

```sh
cli/feature_store_load_test_framework.py \
  --project_id=some_project_id \
  --region=some_region \
  --nouse_gke \
  --target_qps=positive_integer \
  --num_threads=positive_integer \
  --sample_strategy=in_order_or_shuffled_or_random \
  --number_samples=positive_integer \
  --gcs_log_path=gs://some_bucket/path/to/folder
  --nouse_gke
```

The default values are:

*   Target queries per second: 1
*   Number of threads used to send requests: 1
*   Sample strategy: shuffled
*   Number of samples: 10
*   GCS log path: No log

An example of a region would be `us-east4`, and a zone within that region is
`us-east4-a`. See https://cloud.google.com/compute/docs/regions-zones.

#### GKE

To run the tool remotely on GKE as a job:

```sh
python3 feature_store_load_test_framework.py \
  --project_id=some_project_id \
  --region=some_region \
  --target_qps=positive_integer \
  --num_threads=positive_integer \
  --sample_strategy=in_order_or_shuffled_or_random \
  --number_samples=positive_integer \
  --gcs_log_path=gs://some_bucket/path/to/folder \
  --use_gke \
  --cluster_size=positive_integer \
  --cluster_zone=some_zone \
  --keep_cluster=True_or_False \
  --service_account=service_account_to_run_nodes_under
```

The default values are:

*   Target queries per second: 1
*   Number of threads used to send requests (per VM): 1
*   Sample strategy: shuffled
*   Number of samples: 10
*   GCS log path: No log
*   Cluster size: 5
*   Keep cluster: False
*   Service account: Project default compute service account

The target QPS is spread out across all the pods. To save time when using the
tool repeatedly, set `--keep_cluster=True` and see the next section about how to
provide your own cluster.

#### GKE (existing cluster)

To run the tool remotely on an existing GKE cluster call it like this:

```sh
python3 feature_store_load_test_framework.py \
  --project_id=some_project_id \
  --region=some_region \
  --target_qps=positive_integer \
  --sample_strategy=in_order_or_shuffled_or_random \
  --number_samples=positive_integer \
  --gcs_log_path=gs://some_bucket/path/to/folder \
  --use_gke \
  --cluster_name=existing_cluster_name \
  --cluster_size=positive_integer \
  --cluster_zone=some_zone \
  --service_account=service_account_to_run_nodes_under
```

The default values are:

*   Target queries per second: 1
*   Number of threads used to send requests (per VM): 1
*   Sample strategy: shuffled
*   Number of samples: 10
*   GCS log path: No log
*   Cluster size: 5
*   Keep cluster: Doesn't matter what this is set to, will always be kept
*   Service account: Project default compute service account

The target QPS is spread out across all the pods. Set `cluster_size` to how many
copies of the job you would like to run. The recomended value is to make it
equal with the number of pods. The cluster node pool also needs the following
OAuth scope:

*   `https://www.googleapis.com/auth/devstorage.read_only`

### Known Limitations

*   Only one load test can be run at a time because the cluster configuration is
    loaded into the local environment.

## Import Tool

### Dependencies

*   [Python 3](https://www.python.org/downloads)
*   [Abseil](https://abseil.io/docs/python/quickstart)
*   [Google Cloud Python Client](https://cloud.google.com/python/docs/setup)
*   google-cloud-aiplatform
*   google-cloud-storage

### Function

The import tool creates Vertex AI featurestores with a basic setup containing
the entity types, entities, features, and values in the supplied
`featurestore.txt` data file.

### Use

To use the import tool:

```sh
python3 import_tool.py \
  --project_id=some_project_id \
  --region=some_region \
  --gcs_path=some_gcs_path \
  --import_file_path=some_import_file_path
```

Import file path defaults to the included featurestore.txt. You will need a
Google Cloud Storage bucket to stage the feature values in. The GCS path should
be formatted like `gs://bucket_name/folder_path`. This space is only temporarily
used and will be cleaned up upon completion.

### Import/Export Data Format

Each line in a features data file is formatted like:

```
featurestores/{featurestore_id}/entityTypes/{entity_type_id}/entities/{entity_id}/features/{feature}/featureDataTypes/{feature_data_type}/featureValues/{feature_value}
```

The Featurestore data types are
[here](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.featurestores.entityTypes.features#ValueType).
Example:

```
featurestores/hr/entityTypes/employee/entities/bob/features/height_in_centimeters/featureDataTypes/int64/featureValues/1
```

If a value needs to be escaped, for example `/` or the newline, surround it with
double quotes. A features data file is a valid CSV file using the `/` as a
delimiter.

### Known Limitations \ Not Implemented

*   Array value types
*   Multiple IDs for an entity
*   Setting the timestamp value

### Permissions Required

*   Vertex AI Admin
*   Read/write access to a Google Cloud Storage Bucket

### Deleting Existing Datastores

To cleanup your environment after importing a featurestore see this
[page](https://cloud.google.com/vertex-ai/docs/featurestore/managing-featurestores#delete_a_featurestore).

## Export Tool

### Dependencies \ Permissions Required \ Features Data File Format

Same as the [import tool](#import-tool).

### Function

The export tool creates features data file with entity types, entities,
features, and values from Vertex AI featurestores.

### Use

To use the export tool:

```sh
python3 export_tool.py \
  --project_id=some_project_id \
  --region=some_region \
  --gcs_path=some_gcs_path \
  --export_file_path=some_export_file_path \
  --featurestore_id_regex=some_regex \
  --entity_id_regex=some_regex
```

Export file path defaults to export.txt. You will need a Google Cloud Storage
bucket to stage the feature values in. The GS path should be formatted like
`gs://bucket_name/folder_path`. This space is only temporarily used and will be
cleaned up upon completion. To limit which Featurestores are included in the
file, specify a regex to filter Featurestore IDs on. To limit which entity types
are included in the file, specify a regex to filter entity type IDs on.

### Known Limitations \ Not Implemented

*   Array value types
*   Multiple IDs for an entity
*   Setting the timestamp value
