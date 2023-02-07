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

"""Tests for data_importer."""

from absl.testing import absltest

import lib.data_importer as data_importer


class DataImporterTest(absltest.TestCase):

    def get_feature_type(self, type_value: str):
        match type_value:
            case 'INT64':
                return self.feature_pb2_mock.Feature.ValueType.INT64
            case 'BOOL':
                return self.feature_pb2_mock.Feature.ValueType.BOOL
            case 'STRING':
                return self.feature_pb2_mock.Feature.ValueType.STRING
            case _:
                raise KeyError(type_value)

    def setUp(self):
        super().setUp()

        self.entity_type_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'entity_type_pb2', autospec=True))
        # autospec doesn't work on the next one because it says it can't find __getitem__
        self.feature_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'feature_pb2'))
        self.feature_pb2_mock.Feature.ValueType.__getitem__.side_effect = self.get_feature_type
        self.featurestore_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'featurestore_pb2', autospec=True))
        self.featurestore_service_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'featurestore_service_pb2', autospec=True))
        self.io_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'io_pb2', autospec=True))

        uuid_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'uuid4', autospec=True))
        uuid_mock.return_value = 'd3d75afa-a029-4842-9b77-f753661dd1f5'

        self.fsclient_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'FeaturestoreServiceClient', autospec=True))

        self.storage_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'storage', autospec=True))

        self.timestamp_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'Timestamp', autospec=True))
        # set default value for attr created in __init__()
        self.timestamp_mock.return_value.nanos = 0

        # Open is a native method so it can't be autospec'ed
        self.open_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'open'))

        self.csv_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'csv', autospec=True))

        self.stringio_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'StringIO', autospec=True))

        self.logging_mock = self.enter_context(
            absltest.mock.patch.object(data_importer, 'logging', autospec=True))

    def test_empty_file(self):
        self.csv_mock.reader.return_value.__iter__.return_value = []

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.fsclient_mock.assert_not_called()
        self.csv_mock.assert_not_called()

    def test_open_csv(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.open_mock.assert_called_with('featurestore.txt', newline='')
        self.csv_mock.reader.assert_called_with(self.open_mock.return_value.__enter__.return_value, delimiter='/')
        self.csv_mock.reader.return_value.__iter__.assert_called()

    def test_create_featurestore_client(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.fsclient_mock.assert_called_with(
            client_options={"api_endpoint": 'region-aiplatform.googleapis.com'})

    def test_create_featurestore(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        online_serving_config_mock = self.featurestore_pb2_mock.Featurestore.OnlineServingConfig
        online_serving_config_mock.assert_called_with(fixed_node_count=1)
        featurestore_mock = self.featurestore_pb2_mock.Featurestore
        featurestore_mock.assert_called_with(
            online_serving_config=online_serving_config_mock.return_value)
        common_location_mock = self.fsclient_mock.return_value.common_location_path
        common_location_mock.assert_called_with('project-id', 'region')
        fs_request_mock = self.featurestore_service_pb2_mock.CreateFeaturestoreRequest
        fs_request_mock.assert_called_with(parent=common_location_mock.return_value,
                                           featurestore_id='benchmark_featurestore_d3d75',
                                           featurestore=featurestore_mock.return_value)
        self.fsclient_mock.return_value.create_featurestore.assert_called_with(
            fs_request_mock.return_value)

    def test_create_entity_type(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        featurestore_path_mock = self.fsclient_mock.return_value.featurestore_path
        featurestore_path_mock.assert_called_with(
            'project-id', 'region', 'benchmark_featurestore_d3d75')
        entity_type_mock = self.entity_type_pb2_mock.EntityType
        entity_type_mock.assert_called_with(description="")
        entity_type_request_mock = self.featurestore_service_pb2_mock.CreateEntityTypeRequest
        entity_type_request_mock.assert_called_with(parent=featurestore_path_mock.return_value,
                                                    entity_type_id='human',
                                                    entity_type=entity_type_mock.return_value)
        self.fsclient_mock.return_value.create_entity_type.assert_called_with(
            entity_type_request_mock.return_value)

    def test_create_features(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4'],
            ['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'sally', 'features', 'height', '',
              '', 'featureValues', '3'],
            ['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'awake', 'featureDataTypes',
              'bool', 'featureValues', 'True']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        entity_type_path_mock = self.fsclient_mock.return_value.entity_type_path
        entity_type_path_mock.assert_called_with(
            'project-id', 'region', 'benchmark_featurestore_d3d75', 'human')
        feature_mock = self.feature_pb2_mock.Feature
        feature_mock.assert_any_call(
            value_type=self.feature_pb2_mock.Feature.ValueType.INT64, description='',
            disable_monitoring=True)
        feature_request_mock = self.featurestore_service_pb2_mock.CreateFeatureRequest
        feature_request_mock.assert_any_call(
            feature=feature_mock.return_value, feature_id='height')
        feature_mock_2 = self.feature_pb2_mock.Feature
        feature_mock_2.assert_any_call(
            value_type=self.feature_pb2_mock.Feature.ValueType.BOOL, description='',
            disable_monitoring=True)
        feature_request_mock_2 = self.featurestore_service_pb2_mock.CreateFeatureRequest
        feature_request_mock_2.assert_any_call(
            feature=feature_mock_2.return_value, feature_id='height')
        self.fsclient_mock.return_value.batch_create_features.assert_called_with(
            parent=entity_type_path_mock.return_value,
            requests=[feature_request_mock.return_value,
                      feature_request_mock_2.return_value])

    def test_create_storage_client(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.storage_mock.Client.assert_called_with('project-id')

    def test_create_blob_object(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        bucket_mock = self.storage_mock.Client.return_value.bucket
        bucket_mock.assert_called_with('scratch_space')
        blob_mock = bucket_mock.return_value.blob
        blob_mock.assert_called_with('some/path/human_d3d75.csv')

    def test_upload_file(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4'],
            ['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'sally', 'features', 'height', '',
              '', 'featureValues', '3'],
            ['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'awake', 'featureDataTypes',
              'bool', 'featureValues', 'True']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.stringio_mock.assert_called_with(newline='')
        self.csv_mock.writer.assert_called_with(self.stringio_mock.return_value.__enter__.return_value)
        self.csv_mock.writer.return_value.writerow.assert_has_calls([
            absltest.mock.call(['entity_id_d3d75', 'height', 'awake']),
            absltest.mock.call(['bob', '4', 'True']),
            absltest.mock.call(['sally', '3', ''])
        ])
        blob_mock = self.storage_mock.Client.return_value.bucket.return_value.blob.return_value
        blob_mock.upload_from_file.assert_called_with(self.stringio_mock.return_value.__enter__.return_value, rewind=True, content_type='text/csv')

    def test_delete_blob(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        blob_mock = self.storage_mock.Client.return_value.bucket.return_value.blob.return_value
        blob_mock.delete.assert_called_once()

    def test_delete_blob_exception(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4']])
        self.fsclient_mock.return_value.import_feature_values.side_effect = Exception('ow')

        with self.assertRaises(Exception):
            tool = data_importer.DataImporter(
                'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
            tool.execute()

        blob_mock = self.storage_mock.Client.return_value.bucket.return_value.blob.return_value
        blob_mock.delete.assert_called_once()

    def test_ingestion_job(self):
        self.timestamp_mock.return_value.seconds = 1658265670
        self.timestamp_mock.return_value.nanos = 254864000
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64', 'featureValues', '4'],
            ['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'sally', 'features', 'height', '',
              '', 'featureValues', '3'],
            ['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'awake', 'featureDataTypes',
              'bool', 'featureValues', 'True']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        feature_spec_mock = self.featurestore_service_pb2_mock.ImportFeatureValuesRequest.FeatureSpec
        feature_spec_mock.assert_any_call(id='height')
        feature_spec_mock_2 = self.featurestore_service_pb2_mock.ImportFeatureValuesRequest.FeatureSpec
        feature_spec_mock_2.assert_any_call(id='awake')
        gcs_source_mock = self.io_pb2_mock.GcsSource
        gcs_source_mock.assert_called_with(
            uris=['gs://scratch_space/some/path/human_d3d75.csv'])
        csv_source_mock = self.io_pb2_mock.CsvSource
        csv_source_mock.assert_called_with(
            gcs_source=gcs_source_mock.return_value)
        entity_type_path_mock = self.fsclient_mock.return_value.entity_type_path
        entity_type_path_mock.assert_called_with(
            'project-id', 'region', 'benchmark_featurestore_d3d75', 'human')
        import_features_values_mock = self.featurestore_service_pb2_mock.ImportFeatureValuesRequest
        import_features_values_mock.assert_called_with(
            entity_type=entity_type_path_mock.return_value, csv_source=csv_source_mock.return_value,
            disable_online_serving=True, entity_id_field='entity_id_d3d75',
            feature_specs=[feature_spec_mock.return_value, feature_spec_mock_2.return_value],
            feature_time=self.timestamp_mock.return_value, worker_count=1, disable_ingestion_analysis=True)
        self.assertEqual(self.timestamp_mock.return_value.nanos % 1000000, 0)

    def test_failed_parsing(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['bad line']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.logging_mock.warning.assert_called_once()

    def test_bad_datatype(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int63', 'featureValues', '4']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.logging_mock.warning.assert_called_once()

    def test_bad_gs_path(self):
        with self.assertRaises(ValueError):
            tool = data_importer.DataImporter(
                'project-id', 'region', 'featurestore.txt', '//scratch_space/some/path')
            tool.execute()

    def test_array_not_implemented(self):
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['featurestores', 'benchmark_featurestore', 'entityTypes', 'human',
              'entities', 'bob', 'features', 'height', 'featureDataTypes',
              'int64_array', 'featureValues', '1,2,3']])

        tool = data_importer.DataImporter(
            'project-id', 'region', 'featurestore.txt', 'gs://scratch_space/some/path')
        tool.execute()

        self.logging_mock.warning.assert_called_once()


if __name__ == '__main__':
    absltest.main()
