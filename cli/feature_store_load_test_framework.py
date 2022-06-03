#!/usr/bin/env python3

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
"""The FeatureStore load test framework CLI."""

import subprocess

from absl import app
from absl import flags
from lib.framework_runner import FrameworkRunner

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'project_id',
    None,
    'ID of the Google Cloud project you want to load test.',
    required=True,
)
flags.DEFINE_string(
    'feature_query_file_path',
    None,
    'Path to the feature query file.',
    required=True,
)
flags.DEFINE_string(
    'entity_file_path',
    None,
    'Path to the entity file.',
    required=True,
)
flags.DEFINE_integer(
    'target_qps',
    10,
    'Target queries per second. Must be greater than '
    'or equal to the cluster size if running on GKE.',
)
flags.DEFINE_integer(
    'num_threads',
    1,
    'The number of threads to use for sending requests.',
)
flags.DEFINE_enum(
    'sample_strategy',
    'SHUFFLED',
    ['IN_ORDER', 'SHUFFLED'],
    'Sampling strategy.',
    case_sensitive=False,
)
flags.DEFINE_integer(
    'num_warmup_samples',
    5,
    'Number of warmup samples.',
)
flags.DEFINE_integer(
    'num_samples',
    10,
    'Number of samples.',
)
flags.DEFINE_string(
    'region',
    'us-central1',
    'Region you want to load test.',
)
flags.DEFINE_string(
    'gcs_log_path',
    '',
    'Which folder in GCS to write the log files into '
    '(defaults to not writing a log).',
)
flags.DEFINE_bool(
    'use_gke',
    True,
    'Whether to run on GKE or locally (defaults to GKE)',
)
flags.DEFINE_string(
    'cluster_zone',
    'us-central1-a',
    'Zone to create the GKE cluster in.',
)
flags.DEFINE_integer(
    'cluster_size',
    1,
    'Size of the GKE cluster to create.',
)
flags.DEFINE_string(
    'existing_cluster_name',
    None,
    'Name of existing cluster to use.',
)
flags.DEFINE_bool(
    'keep_cluster',
    False,
    'Whether to keep the cluster at the end of the run (defaults to False).',
)
flags.DEFINE_string(
    'service_account',
    '',
    'Service account to run the nodes under.',
)
flags.DEFINE_string(
    'image_url',
    '',
    'The container image to use in GKE mode.',
)

flags.register_validator(
    'target_qps',
    lambda value: value > 0,
    message='--target_qps must be a positive number',
)
flags.register_validator(
    'num_threads',
    lambda value: value > 0,
    message='--num_threads must be a positive number',
)
flags.register_validator(
    'num_samples',
    lambda value: value > 0,
    message='--num_samples must be a positive number',
)
flags.register_validator(
    'num_warmup_samples',
    lambda value: value >= 0,
    message='--num_warmup_samples must be a positive number or zero',
)
flags.DEFINE_string(
    'existing_biqquery_dataset_id',
    '',
    'The existing BigQuery dataset id for output.',
)


def main(argv):
  del argv  # unused

  if FLAGS.use_gke:
    if not FLAGS.image_url:
      raise ValueError('--image_url must be provided when using GKE.')

    try:
      runner = FrameworkRunner(
          _project_id=FLAGS.project_id,
          _region=FLAGS.region,
          _target_qps=FLAGS.target_qps,
          _num_threads=FLAGS.num_threads,
          _sample_strategy=FLAGS.sample_strategy,
          _num_warmup_samples=FLAGS.num_warmup_samples,
          _num_samples=FLAGS.num_samples,
          _gcs_log_path=FLAGS.gcs_log_path,
          _cluster_zone=FLAGS.cluster_zone,
          _cluster_size=FLAGS.cluster_size,
          _existing_cluster_name=FLAGS.existing_cluster_name,
          _keep_cluster=FLAGS.keep_cluster,
          _service_account=FLAGS.service_account,
          _feature_query_file_path=FLAGS.feature_query_file_path,
          _entity_file_path=FLAGS.entity_file_path,
          _image_url=FLAGS.image_url,
          _dataset_id=FLAGS.existing_biqquery_dataset_id,
      )
      runner.execute()
    except Exception as ex:
      raise Exception(
          f'Failed to execute FeatureStore load testing framework.') from ex
  else:
    subprocess.run(
        [
            './gradlew', 'run', f'--args='
            f'--target_qps={str(FLAGS.target_qps)} '
            f'--num_threads={str(FLAGS.num_threads)} '
            f'--sample_strategy={FLAGS.sample_strategy} '
            f'--num_warmup_samples={FLAGS.num_warmup_samples} '
            f'--num_samples={str(FLAGS.num_samples)} '
            f'--project_id={FLAGS.project_id} '
            f'--region={FLAGS.region} '
            f'--gcs_output_path={FLAGS.gcs_log_path} '
            f'--feature_query_file={FLAGS.feature_query_file_path} '
            f'--entity_file={FLAGS.entity_file_path} '
            f'--bigquery_output_dataset={FLAGS.existing_biqquery_dataset_id} '
        ],
        cwd='../worker',
        check=True,
    )


if __name__ == '__main__':
  app.run(main)
