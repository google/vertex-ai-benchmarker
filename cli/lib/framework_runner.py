# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import dataclasses
import os
from pathlib import Path
import subprocess
from tempfile import mkstemp
from time import sleep
from typing import Any
from uuid import uuid4

from absl import logging
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from kubernetes import client, config, utils

WAIT_TIME_PER_ITERATION_SECONDS = 15
MAX_WAIT_TIME_SECONDS = 1800  # 30 minutes


@dataclasses.dataclass
class FrameworkRunner:
  """Runs the FeatureStore load test framework on a GKE cluster.

    Args:
        project_id: ID of the Google Cloud project you want to load test.
        region: Region you want to load test.
        target_qps: Target queries per second.
        num_threads: Number of threads to use per VM.
        sample_strategy: Sampling strategy.
        num_warmup_samples: Number of warmup samples.
        num_samples: Number of samples.
        gcs_log_path: Which folder in GCS to write the log files into.
        cluster_zone: Zone to create the GKE cluster in.
        cluster_size: Size of the GKE cluster to create.
        existing_cluster_name: Name of existing cluster.
        keep_cluster: Whether to keep the cluster at the end of the run.
        service_account: Service account to run nodes under.
        feature_query_file_path: Path to the feature query file.
        entity_file_path: Path to the entity file.
        image_url: Container image URL for the image that will be run on GKE.
  """
  _project_id: str
  _region: str
  _target_qps: int
  _num_threads: int
  _sample_strategy: str
  _num_warmup_samples: int
  _num_samples: int
  _gcs_log_path: str
  _cluster_zone: str
  _cluster_size: str
  _existing_cluster_name: str
  _keep_cluster: bool
  _service_account: str
  _feature_query_file_path: str
  _entity_file_path: str
  _image_url: str
  _dataset_id: str = ""

  def execute(self):
    """Executes the framework runner."""
    if self._cluster_size > self._target_qps:
      raise Exception(f'Cluster size `{self._cluster_size}` '
                      f"must be smaller than target QPS `{self._target_qps}'.")

    if self._dataset_id == "":
      self._dataset_id = self._create_bq_dataset()

    gke_client = build('container', 'v1')

    cluster_parent = f'projects/{self._project_id}/locations/{self._cluster_zone}'
    if self._existing_cluster_name:
      cluster_name = self._existing_cluster_name
    else:
      cluster_name = self._create_cluster(cluster_parent, gke_client)

    try:
      self._load_credentials(cluster_name)
      jobs = self._create_jobs()

      for job_name in jobs:
        self._wait_for_job_to_complete(job_name)
    finally:
      # Don't delete the cluster if the user wants to keep it
      # or the cluster was user supplied
      if not self._keep_cluster and not self._existing_cluster_name:
        self._delete_cluster(gke_client, cluster_parent, cluster_name)

  def _create_bq_dataset(self):
    """Create the BigQuery dataset for the test"""
    # Construct a BigQuery client object.
    client = bigquery.Client()
    dataset_id = f'{self._project_id}.vertex_ai_benchmarker_results_{self._target_qps}_qps_{str(uuid4())[:5]}'

    # Check if dataset exists. If not, create a new one.
    try:
        dataset = client.get_dataset(dataset_id)  # Make an API request.
    except NotFound:
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self._region

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.

    return dataset_id

  def _create_jobs(self):
    """Create the jobs to run on GKE.

        Returns:
            List of the names of the jobs.
    """
    jobs = []
    # If the target QPS is evenly divisible by the cluster size, only one job is needed.
    # Otherwise create a second job for only the last pod and put the remainder QPS in there.
    if self._target_qps % self._cluster_size == 0:
      job_name = self._create_job(self._cluster_size,
                                  self._target_qps // self._cluster_size)
      jobs.append(job_name)
    else:
      primary_job_qps = self._target_qps // self._cluster_size
      primary_job_name = self._create_job(self._cluster_size - 1,
                                          primary_job_qps)
      jobs.append(primary_job_name)

      secondary_job_qps = primary_job_qps + self._target_qps % self._cluster_size
      secondary_job_name = self._create_job(1, secondary_job_qps)
      jobs.append(secondary_job_name)
    return jobs

  def _create_job(self, pods: int, target_qps: int):
    """Create a job to run on GKE.

        Args:
            pods: Number of pods to assign job to.
            target_qps: Targetted queries per second.

        Returns:
            Name of the job.
    """
    job_name = f'fsloadtest-{str(uuid4())[:5]}-job'
    logging.info(f'Creating job `{job_name}`...')

    temp_filepath = self._write_job_file(job_name, pods, target_qps)

    try:
      kubernetes_client = client.ApiClient()
      utils.create_from_yaml(kubernetes_client, temp_filepath)
    finally:
      os.remove(temp_filepath)
    return job_name

  def _load_credentials(self, cluster_name: str):
    """Load the credentials for a GKE cluster.

        Args:
            cluster_name: Name of the GKE cluster.
    """
    subprocess.run([
        'gcloud', 'container', 'clusters', 'get-credentials', cluster_name,
        f'--project={self._project_id}', f'--zone={self._cluster_zone}'
    ],
                   check=True)
    config.load_kube_config()

  def _wait_for_job_to_complete(self, job_name: str):
    """Wait for a job to complete on GKE.

        Args:
            job_name: Name of the job to wait for.
    """
    logging.info(f'Waiting for job to complete...')
    batch_api = client.BatchV1Api()
    job_status = batch_api.read_namespaced_job_status(job_name, 'default')
    current_wait = 0
    while job_status.status.failed is None and job_status.status.succeeded is None:
      current_wait = self._sleep_with_timeout(current_wait)
      job_status = batch_api.read_namespaced_job_status(job_name, 'default')

    logging.info(f'Job completed with status `{job_status.status}`.')

  def _delete_cluster(self, gke_client: Any, parent: str, cluster_name: str):
    """Delete a cluster on GKE.

        Args:
            gke_client: GKE client.
            parent: Parent path of cluster.
            cluster_name: Name of cluster.
    """
    cluster_path = f'{parent}/clusters/{cluster_name}'
    logging.info(f'Deleting cluster `{cluster_name}`...')

    delete_operation = gke_client.projects().locations().clusters().delete(
        name=cluster_path).execute()

    self._wait_for_operation(gke_client, parent, delete_operation)

  def _write_job_file(self, job_name: str, pods: int, target_qps: int) -> str:
    """Write job file to be uploaded to GKE.

        Args:
            job_name: Name of the job.
            pods: Number of pods to assign job to.
            target_qps: Targetted queries per second.

        Returns:
            Path to the job file.
    """
    # Create a file in temporary space
    temp_file_handle, temp_filepath = mkstemp(text=True)

    job_flags = {
        'job_name': job_name,
        'pods': str(pods),
        'image_url': self._image_url,
        'project_id': self._project_id,
    }

    feature_query_file_path = self._feature_query_file_path

    if feature_query_file_path.startswith('gs://'):
      # Example file provided for debugging purposes
      job_flags['feature_query_file_path'] = 'example_query_file.textproto'
      job_flags['feature_query_file_content'] = _base64_content(
          'data/query_file.textproto')
    else:
      job_flags['feature_query_file_path'] = feature_query_file_path.lstrip('/')
      job_flags['feature_query_file_content'] = _base64_content(
          feature_query_file_path)
      feature_query_file_path = os.path.join(
          '/config',
          feature_query_file_path.lstrip('/'),
      )

    entity_file_path = self._entity_file_path

    if entity_file_path.startswith('gs://'):
      # Example file provided for debugging purposes
      job_flags['entity_file_path'] = 'example_entity_file.txt'
      job_flags['entity_file_content'] = _base64_content('data/entity_file.txt')
    else:
      job_flags['entity_file_path'] = entity_file_path.lstrip('/')
      job_flags['entity_file_content'] = _base64_content(entity_file_path)
      entity_file_path = os.path.join('/config', entity_file_path.lstrip('/'))

    # Determine the values for the job template
    job_flags['args']= (
       f'--target_qps={str(target_qps)} ' \
       f'--num_threads={str(self._num_threads)} ' \
       f'--sample_strategy={self._sample_strategy} ' \
       f'--num_samples={str(self._num_samples)} ' \
       f'--project_id={self._project_id} ' \
       f'--region={self._region} ' \
       f'--gcs_output_path={self._gcs_log_path} ' \
       f'--feature_query_file={feature_query_file_path} ' \
       f'--entity_file={entity_file_path} ' \
       f'--num_warmup_samples={self._num_warmup_samples} ' \
       f'--bigquery_output_dataset={self._dataset_id}'
    )

    # Copy the template while filling out the flags
    with open('data/job_template.yaml', 'r') as template_file:
      with os.fdopen(temp_file_handle, 'w') as temp_file:
        while template_line := template_file.readline():
          for keyword, replacement in job_flags.items():
            replace_keyword = f'<<{keyword}>>'
            template_line = template_line.replace(replace_keyword, replacement)
          temp_file.write(template_line)
        temp_file.flush()

    return temp_filepath

  def _create_cluster(self, cluster_parent: str, gke_client: Any):
    """Create a cluster on GKE.

        Args:
            cluster_parent: Parent path of cluster.
            gke_client: GKE client.

        Returns:
            Name of cluster.
    """
    cluster_name = f'fsloadtest-{str(uuid4())[:5]}-cluster'
    logging.info(f'Creating cluster `{cluster_name}`...')

    config = {
        'oauthScopes': [
            # need this to interact with FeatureStore
            'https://www.googleapis.com/auth/cloud-platform'
        ]
    }
    config.update({'serviceAccount': self._service_account} if self
                  ._service_account else {})

    node_pool = [{
        'name': 'read-storage',
        'initialNodeCount': self._cluster_size,
        'config': config
    }]

    create_operation = gke_client.projects().locations().clusters().create(
        parent=cluster_parent,
        body={
            'parent': cluster_parent,
            'cluster': {
                'name': cluster_name,
                'nodePools': node_pool
            }
        }).execute()

    self._wait_for_operation(gke_client, cluster_parent, create_operation)
    return cluster_name

  def _wait_for_operation(self, gke_client: Any, cluster_parent: str,
                          operation: Any):
    """Wait for an operation to complete.

        Args:
            gke_client: GKE client.
            cluster_parent: Parent path of the cluster the operation is in.
            operation: Operation to wait on for completion.
    """
    operation_name = f"{cluster_parent}/operations/{operation['name']}"
    operation_status = operation['status']
    current_wait = 0
    while operation_status != 'DONE':
      current_wait = self._sleep_with_timeout(current_wait)
      operation_status = gke_client.projects().locations().operations().get(
          name=operation_name).execute()['status']

  def _sleep_with_timeout(self, current_wait: int) -> int:
    """Sleep with a timeout to prevent getting permanently stuck.

        Args: Current amount of time spent sleeping.

        Returns:
            End amount of time spent sleeping.

        Raises:
            Exception if the maximum amount of wait time is exceeded.
    """
    sleep(WAIT_TIME_PER_ITERATION_SECONDS)
    current_wait += WAIT_TIME_PER_ITERATION_SECONDS
    if current_wait > MAX_WAIT_TIME_SECONDS:
      raise Exception('Maximum wait time exceeded')
    return current_wait


def _base64_content(path: str) -> str:
  base64_bytes = Path(path).read_text().encode('ascii')
  return base64.b64encode(base64_bytes).decode('ascii')
