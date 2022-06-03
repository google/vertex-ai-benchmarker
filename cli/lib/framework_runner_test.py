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
"""Tests for framework_runner."""

import os

from absl.testing import absltest
from google.cloud.exceptions import NotFound
import lib.framework_runner as framework_runner


class FrameworkRunnerTest(absltest.TestCase):

  _DEFAULT_PROJ = 'project'

  def setUp(self):
    super().setUp()

    # Open is a native method that can't be autospec'ed
    self.open_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'open'))
    self.open_mock.return_value.__enter__().readline.return_value = ''

    self.fdopen_mock = self.enter_context(
        absltest.mock.patch.object(os, 'fdopen', autospec=True))
    self.fdopen_text = ''
    self.fdopen_mock.return_value.__enter__(
    ).write.side_effect = self.append_fdopen_text

    self.remove_mock = self.enter_context(
        absltest.mock.patch.object(os, 'remove', autospec=True))

    self.mkstemp_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'mkstemp', autospec=True))
    self.mkstemp_mock.return_value = 0, '/temp'

    uuid_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'uuid4', autospec=True))
    uuid_mock.return_value = 'd3d75afa-a029-4842-9b77-f753661dd1f5'

    self.sleep_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'sleep', autospec=True))
    self.subprocess_mock = self.enter_context(
        absltest.mock.patch.object(
            framework_runner, 'subprocess', autospec=True))

    self.enter_context(
        absltest.mock.patch.object(framework_runner, 'logging', autospec=True))

    self.build_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'build', autospec=True))
    self.build_mock.return_value.projects().locations().clusters().create(
    ).execute(
    ).__getitem__.side_effect = lambda x: 'create-operation' if x == 'name' else 'DONE'
    self.build_mock.return_value.projects().locations().clusters().delete(
    ).execute(
    ).__getitem__.side_effect = lambda x: 'delete-operation' if x == 'name' else 'DONE'
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.return_value = 'DONE'

    self.client_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'client', autospec=True))
    self.job_status_mock = absltest.mock.MagicMock()
    self.job_status_mock.status.succeeded = False
    self.job_status_mock.status.failed = True
    self.client_mock.BatchV1Api.return_value.read_namespaced_job_status.return_value = self.job_status_mock

    self.config_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'config', autospec=True))
    self.utils_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'utils', autospec=True))

    self.bq_client_mock = absltest.mock.MagicMock()
    self.bq_client_mock.get_dataset.side_effect = NotFound('dataset not found')
    self.bq_client_mock.create_dataset.return_value = absltest.mock.MagicMock()
    self.bq_mock = self.enter_context(
        absltest.mock.patch.object(framework_runner, 'bigquery', autospec=True))
    self.bq_mock.Client.return_value = self.bq_client_mock
    self.dataset_mock = absltest.mock.MagicMock()
    self.bq_mock.Dataset.return_value = self.dataset_mock

  def _minimal_initialization(self) -> framework_runner.FrameworkRunner:
    return framework_runner.FrameworkRunner(
        _project_id=self._DEFAULT_PROJ,
        _region='region',
        _target_qps=5,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='',
        _keep_cluster=False,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')

  def append_fdopen_text(self, text: str):
    self.fdopen_text += text

  def test_target_qps_too_low_exception(self):
    tool = framework_runner.FrameworkRunner(
        _project_id=self._DEFAULT_PROJ,
        _region='region',
        _target_qps=2,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='',
        _keep_cluster=False,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')
    with self.assertRaises(Exception):
      tool.execute()

  def test_build_client(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.build_mock.assert_called_with('container', 'v1')

  def test_bq_client(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.bq_mock.Client.assert_called_with()
    self.bq_client_mock.get_dataset.assert_called_with(
        'vertex_ai_benchmarker_results_5_qps_d3d75')
    self.bq_mock.Dataset.assert_called_with(
        'vertex_ai_benchmarker_results_5_qps_d3d75')
    self.bq_client_mock.create_dataset.assert_called_with(
        self.dataset_mock, timeout=30)

  def test_kubernetes_client(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.client_mock.ApiClient.assert_called()

  def test_kubernetes_batch_client(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.client_mock.BatchV1Api.assert_called()

  def test_create_cluster(self):
    tool = self._minimal_initialization()
    tool.execute()

    node_pool = [{
        'name': 'read-storage',
        'initialNodeCount': 5,
        'config': {
            'oauthScopes': ['https://www.googleapis.com/auth/cloud-platform']
        }
    }]
    self.build_mock.return_value.projects().locations().clusters(
    ).create.assert_called_with(
        parent='projects/project/locations/zone',
        body={
            'parent': 'projects/project/locations/zone',
            'cluster': {
                'name': 'fsloadtest-d3d75-cluster',
                'nodePools': node_pool
            }
        })
    self.build_mock.return_value.projects().locations().clusters().create(
    ).execute.assert_called()

  def test_create_cluster_with_service_account(self):
    tool = framework_runner.FrameworkRunner(
        _project_id=self._DEFAULT_PROJ,
        _region='region',
        _target_qps=5,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='',
        _keep_cluster=False,
        _service_account='service-account',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')
    tool.execute()

    node_pool = [{
        'name': 'read-storage',
        'initialNodeCount': 5,
        'config': {
            'oauthScopes': ['https://www.googleapis.com/auth/cloud-platform'],
            'serviceAccount': 'service-account'
        }
    }]
    self.build_mock.return_value.projects().locations().clusters(
    ).create.assert_called_with(
        parent='projects/project/locations/zone',
        body={
            'parent': 'projects/project/locations/zone',
            'cluster': {
                'name': 'fsloadtest-d3d75-cluster',
                'nodePools': node_pool
            }
        })
    self.build_mock.return_value.projects().locations().clusters().create(
    ).execute.assert_called()

  def test_wait_for_cluster_creation(self):
    self.build_mock.return_value.projects().locations().clusters().create(
    ).execute(
    ).__getitem__.side_effect = lambda x: 'create-operation' if x == 'name' else 'IN PROGRESS'
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.side_effect = ['IN PROGRESS', 'DONE']

    tool = self._minimal_initialization()
    tool.execute()

    self.build_mock.return_value.projects().locations().operations(
    ).get.assert_any_call(
        name='projects/project/locations/zone/operations/create-operation')
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.assert_called()
    self.sleep_mock.assert_has_calls(
        [absltest.mock.call(15), absltest.mock.call(15)])

  def test_wait_for_cluster_creation_timeout(self):
    self.build_mock.return_value.projects().locations().clusters().create(
    ).execute(
    ).__getitem__.side_effect = lambda x: 'create-operation' if x == 'name' else 'IN PROGRESS'
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.return_value = 'IN PROGRESS'

    tool = self._minimal_initialization()
    with self.assertRaises(Exception):
      tool.execute()

  def test_delete_cluster(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.build_mock.return_value.projects().locations().clusters(
    ).delete.assert_called_with(
        name='projects/project/locations/zone/clusters/fsloadtest-d3d75-cluster'
    )
    self.build_mock.return_value.projects().locations().clusters().delete(
    ).execute.assert_called()

  def test_delete_cluster_exception(self):
    self.config_mock.load_kube_config.side_effect = Exception('ouch')

    tool = self._minimal_initialization()
    with self.assertRaises(Exception):
      tool.execute()

    self.build_mock.return_value.projects().locations().clusters(
    ).delete.assert_called_with(
        name='projects/project/locations/zone/clusters/fsloadtest-d3d75-cluster'
    )
    self.build_mock.return_value.projects().locations().clusters().delete(
    ).execute.assert_called()

  def test_wait_for_cluster_deletion(self):
    self.build_mock.return_value.projects().locations().clusters().delete(
    ).execute(
    ).__getitem__.side_effect = lambda x: 'delete-operation' if x == 'name' else 'IN PROGRESS'
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.side_effect = ['IN PROGRESS', 'DONE']

    tool = self._minimal_initialization()
    tool.execute()

    self.build_mock.return_value.projects().locations().operations(
    ).get.assert_any_call(
        name='projects/project/locations/zone/operations/delete-operation')
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.assert_called()
    self.sleep_mock.assert_has_calls(
        [absltest.mock.call(15), absltest.mock.call(15)])

  def test_wait_for_cluster_deletion_timeout(self):
    self.build_mock.return_value.projects().locations().clusters().delete(
    ).execute(
    ).__getitem__.side_effect = lambda x: 'delete-operation' if x == 'name' else 'IN PROGRESS'
    self.build_mock.return_value.projects().locations().operations().get(
    ).execute().__getitem__.return_value = 'IN PROGRESS'

    tool = self._minimal_initialization()
    with self.assertRaises(Exception):
      tool.execute()

  def test_keep_cluster(self):
    tool = framework_runner.FrameworkRunner(
        _project_id=self._DEFAULT_PROJ,
        _region='region',
        _target_qps=5,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='',
        _keep_cluster=True,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')
    tool.execute()

    with self.assertRaises(AssertionError):
      # Unfortunately assert_not_called doesn't work because there is a false positive call
      self.build_mock.return_value.projects().locations().clusters(
      ).delete.assert_called_with(
          name='projects/project/locations/zone/clusters/fsloadtest-d3d75-cluster'
      )

  def test_keep_named_cluster(self):
    tool = framework_runner.FrameworkRunner(
        _project_id=self._DEFAULT_PROJ,
        _region='region',
        _target_qps=5,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='my-cluster',
        _keep_cluster=False,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')
    tool.execute()

    with self.assertRaises(AssertionError):
      # Unfortunately assert_not_called doesn't work because there is a false positive call
      self.build_mock.return_value.projects().locations().clusters(
      ).delete.assert_called_with(
          name='projects/project/locations/zone/clusters/my-cluster')

  def test_run_gcloud_get_credentials(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.subprocess_mock.run.assert_called_with([
        'gcloud', 'container', 'clusters', 'get-credentials',
        'fsloadtest-d3d75-cluster', f'--project={self._DEFAULT_PROJ}',
        '--zone=zone'
    ],
                                                check=True)
    self.config_mock.load_kube_config.assert_called_once()

  def test_run_gcloud_get_credentials_custom_cluster(self):
    tool = framework_runner.FrameworkRunner(
        _project_id=self._DEFAULT_PROJ,
        _region='region',
        _target_qps=5,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='cluster',
        _keep_cluster=False,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')
    tool.execute()

    self.subprocess_mock.run.assert_called_with([
        'gcloud', 'container', 'clusters', 'get-credentials', 'cluster',
        f'--project={self._DEFAULT_PROJ}', '--zone=zone'
    ],
                                                check=True)
    self.config_mock.load_kube_config.assert_called_once()

  def test_write_job_file_include_entity_file_arguments(self):
    self.open_mock.return_value.__enter__().readline.side_effect = [
        '<<job_name>> <<pods>> '
        '<<image_url>> <<args>> <<project_id>> '
        '<<feature_query_file_path>> <<feature_query_file_content>> '
        '<<entity_file_path>> <<entity_file_content>>', ''
    ]

    tool = framework_runner.FrameworkRunner(
        _project_id='project',
        _region='region',
        _target_qps=5,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='log-file-path',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='',
        _keep_cluster=False,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='gs://bucket/data/entity_file.txt',
        _image_url='image-url')
    tool.execute()

    self.open_mock.assert_called_with('data/job_template.yaml', 'r')
    self.open_mock.return_value.__enter__().readline.assert_called()
    self.open_mock.return_value.__exit__.assert_called()

    self.fdopen_mock.assert_called_with(0, 'w')
    self.fdopen_mock.return_value.__enter__().write.assert_called()
    self.fdopen_mock.return_value.__enter__().flush.assert_called()
    self.fdopen_mock.return_value.__exit__.assert_called()
    self.assertEqual(
        'fsloadtest-d3d75-job 5 image-url --target_qps=1 '
        '--num_threads=2 --sample_strategy=strategy '
        '--num_samples=10 --project_id=project '
        '--region=region --gcs_output_path=log-file-path '
        '--feature_query_file=/config/data/query_file.textproto '
        '--entity_file=gs://bucket/data/entity_file.txt --num_warmup_samples=5 '
        '--bigquery_output_dataset=vertex_ai_benchmarker_results_5_qps_d3d75 project '
        'data/query_file.textproto '
        'IyBwcm90by1maWxlOiBmZWF0dXJlc3RvcmVfb25saW5lX3NlcnZpY2UucHJvdG8KIyBwcm90by1tZXNzYWdlOiBSZXF1ZXN0cwoKcmVxdWVzdHNfcGVyX2ZlYXR1cmVzdG9yZTogewogIGZlYXR1cmVzdG9yZV9pZDogImJlbmNobWFya19mZWF0dXJlc3RvcmVfYWJjMTIzIgogIHJlcXVlc3RzOiB7CiAgICByZWFkX2ZlYXR1cmVfdmFsdWVzX3JlcXVlc3Q6IHsKICAgICAgZW50aXR5X3R5cGU6ICJodW1hbiIKICAgICAgZW50aXR5X2lkOiAiJHtFTlRJVFlfSUR9IgogICAgICBmZWF0dXJlX3NlbGVjdG9yOiB7CiAgICAgICAgaWRfbWF0Y2hlcjogewogICAgICAgICAgaWRzOiBbIioiXQogICAgICAgIH0KICAgICAgfQogICAgfQogIH0KfQo= '
        'example_entity_file.txt '
        'ZmVhdHVyZXN0b3Jlcy9iZW5jaG1hcmtfZmVhdHVyZXN0b3JlX2FiYzEyMy9lbnRpdHlUeXBlcy9odW1hbi9lbnRpdHkvdXNlcl9hQGdtYWlsLmNvbQpmZWF0dXJlc3RvcmVzL2JlbmNobWFya19mZWF0dXJlc3RvcmVfYWJjMTIzL2VudGl0eVR5cGVzL2h1bWFuL2VudGl0eS91c2VyX2JAZ21haWwuY29tCmZlYXR1cmVzdG9yZXMvYmVuY2htYXJrX2ZlYXR1cmVzdG9yZV9hYmMxMjMvZW50aXR5VHlwZXMvaHVtYW4vZW50aXR5L3VzZXJfY0BnbWFpbC5jb20K',
        self.fdopen_text)

  def test_write_multiple_job_files(self):
    self.open_mock.return_value.__enter__().readline.side_effect = [
        '<<job_name>> <<pods>> '
        '<<image_url>> <<args>> <<project_id>>', '',
        '<<job_name>> <<pods>> <<image_url>> <<args>> <<project_id>>', ''
    ]

    tool = framework_runner.FrameworkRunner(
        _project_id='project',
        _region='region',
        _target_qps=7,
        _num_threads=2,
        _sample_strategy='strategy',
        _num_warmup_samples=5,
        _num_samples=10,
        _gcs_log_path='log-file-path',
        _cluster_zone='zone',
        _cluster_size=5,
        _existing_cluster_name='',
        _keep_cluster=False,
        _service_account='',
        _feature_query_file_path='data/query_file.textproto',
        _entity_file_path='data/entity_file.txt',
        _image_url='image-url')
    tool.execute()

    self.open_mock.assert_called_with('data/job_template.yaml', 'r')
    self.open_mock.return_value.__enter__().readline.assert_called()
    self.open_mock.return_value.__exit__.assert_called()

    self.fdopen_mock.assert_called_with(0, 'w')
    self.fdopen_mock.return_value.__enter__().write.assert_called()
    self.fdopen_mock.return_value.__enter__().flush.assert_called()
    self.fdopen_mock.return_value.__exit__.assert_called()
    self.assertEqual(
        'fsloadtest-d3d75-job 4 image-url --target_qps=1 '
        '--num_threads=2 --sample_strategy=strategy '
        '--num_samples=10 --project_id=project --region=region '
        '--gcs_output_path=log-file-path '
        '--feature_query_file=/config/data/query_file.textproto '
        '--entity_file=/config/data/entity_file.txt '
        '--num_warmup_samples=5 '
        '--bigquery_output_dataset=vertex_ai_benchmarker_results_7_qps_d3d75 '
        'projectfsloadtest-d3d75-job 1 image-url --target_qps=3 '
        '--num_threads=2 --sample_strategy=strategy '
        '--num_samples=10 --project_id=project --region=region '
        '--gcs_output_path=log-file-path '
        '--feature_query_file=/config/data/query_file.textproto '
        '--entity_file=/config/data/entity_file.txt '
        '--num_warmup_samples=5 '
        '--bigquery_output_dataset=vertex_ai_benchmarker_results_7_qps_d3d75 '
        'project',
        self.fdopen_text,
    )

  def test_create_job_from_yaml(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.utils_mock.create_from_yaml.assert_called_with(
        self.client_mock.ApiClient.return_value, '/temp')

  def test_delete_temp_file(self):
    tool = self._minimal_initialization()
    tool.execute()

    self.remove_mock.assert_called_with('/temp')

  def test_delete_temp_file_exception(self):
    self.utils_mock.create_from_yaml.side_effect = Exception('ouch')

    tool = self._minimal_initialization()
    with self.assertRaises(Exception):
      tool.execute()

    self.remove_mock.assert_called_with('/temp')

  def test_wait_for_job_to_complete(self):
    job_status_wait = absltest.mock.MagicMock()
    job_status_wait.status.succeeded = None
    job_status_wait.status.failed = None
    job_status_succeeded = absltest.mock.MagicMock()
    job_status_succeeded.status.succeeded = True
    job_status_succeeded.status.failed = False
    self.client_mock.BatchV1Api.return_value.read_namespaced_job_status.side_effect = [
        job_status_wait, job_status_succeeded
    ]

    tool = self._minimal_initialization()
    tool.execute()

    self.client_mock.BatchV1Api().read_namespaced_job_status.assert_called_with(
        'fsloadtest-d3d75-job', 'default')
    self.sleep_mock.assert_called_once()

  def test_wait_for_job_to_timeout(self):
    job_status_wait = absltest.mock.MagicMock()
    job_status_wait.status.succeeded = None
    job_status_wait.status.failed = None
    job_status_succeeded = absltest.mock.MagicMock()
    job_status_succeeded.status.succeeded = True
    job_status_succeeded.status.failed = False
    self.client_mock.BatchV1Api.return_value.read_namespaced_job_status.return_value = job_status_wait

    tool = self._minimal_initialization()
    with self.assertRaises(Exception):
      tool.execute()


if __name__ == '__main__':
  absltest.main()
