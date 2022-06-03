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

"""Tests for data_exporter."""

from absl.testing import absltest

from google.cloud.aiplatform_v1.types import feature as feature_pb2
from google.cloud.aiplatform_v1.types import entity_type as entity_type_pb2
from google.cloud.aiplatform_v1.types import featurestore as featurestore_pb2

import lib.data_exporter as data_exporter


class DataExporterTest(absltest.TestCase):

    def setUp(self):
        super().setUp()

        self.entity_type_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'entity_type_pb2', autospec=True))
        self.entity_type_mock = self.entity_type_pb2_mock.return_value
        self.entity_type_mock.name = 'projects/project-number/locations/region/featurestores/featurestore-id/entityTypes/entity-type-id'

        self.featurestore_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'featurestore_pb2', autospec=True))
        self.featurestore_mock = self.featurestore_pb2_mock.return_value
        self.featurestore_mock.name = 'projects/project-number/locations/region/featurestores/featurestore-id'

        self.feature_mock = self.enter_context(absltest.mock.patch.object(feature_pb2, 'Feature'))
        self.feature_mock.name = 'projects/project-number/locations/region/featurestores/featurestore-id/entityTypes/entity-type-id/features/feature-id'
        self.feature_mock.value_type.name = 'STRING'

        self.featurestore_service_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'featurestore_service_pb2', autospec=True))
        self.export_feature_values_request_mock = self.featurestore_service_pb2_mock.ExportFeatureValuesRequest
        self.full_export = self.featurestore_service_pb2_mock.ExportFeatureValuesRequest.FullExport
        self.feature_value_destination_mock = self.featurestore_service_pb2_mock.FeatureValueDestination

        self.io_pb2_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'io_pb2', autospec=True))

        self.feature_selector_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'FeatureSelector', autospec=True))
        self.id_matcher_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'IdMatcher', autospec=True))

        uuid_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'uuid4', autospec=True))
        uuid_mock.return_value = 'd3d75afa-a029-4842-9b77-f753661dd1f5'

        self.fsclient_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'FeaturestoreServiceClient', autospec=True))
        self.fsclient_mock.return_value.list_featurestores.return_value = [self.featurestore_mock]
        self.fsclient_mock.return_value.list_entity_types.return_value = [self.entity_type_mock]
        self.fsclient_mock.return_value.list_features.return_value = [self.feature_mock]

        self.storage_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'storage', autospec=True))
        self.storage_bucket_mock = self.storage_mock.Client.return_value.bucket
        self.storage_blob_mock = self.storage_mock.Client.return_value.bucket.return_value.blob
        self.storage_blob_content_mock = self.storage_mock.Client.return_value.bucket.return_value.blob.return_value.download_as_bytes.return_value.decode

        # Open is a native method so it can't be autospec'ed
        self.open_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'open'))

        self.csv_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'csv', autospec=True))
        self.csv_mock.reader.return_value.__iter__.return_value = iter(
            [['feature_timestamp', 'entity_type_entity-type-id', 'feature-id'],
             ['2022-08-02 21:30:40.761 UTC', 'entity-id', 'feature-value']]
        )

        self.stringio_mock = self.enter_context(
            absltest.mock.patch.object(data_exporter, 'StringIO', autospec=True))

        self.enter_context(
            absltest.mock.patch.object(data_exporter, 'logging', autospec=True))

    def test_create_featurestore_client(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.fsclient_mock.assert_called_with(client_options={"api_endpoint": "region-aiplatform.googleapis.com"})

    def test_list_featurestores(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        common_location_path_mock = self.fsclient_mock.return_value.common_location_path
        common_location_path_mock.assert_called_with('project-id', 'region')
        self.fsclient_mock.return_value.list_featurestores.assert_called_with(
            parent=common_location_path_mock.return_value)

    def test_list_entity_types(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.fsclient_mock.return_value.list_entity_types.assert_called_with(
            parent=self.featurestore_mock.name)

    def test_list_features(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.fsclient_mock.return_value.list_features.assert_called_with(
            parent=self.entity_type_mock.name)

    def test_create_storage_client(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.storage_mock.Client.assert_called_with('project-id')

    def test_storage_get_bucket(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.storage_bucket_mock.assert_called_with('scratch-bucket')

    def test_storage_get_blob(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.storage_mock.assert_not_called()
        self.storage_blob_mock.assert_called_with('entity-type-id_d3d75/000000000000.csv')

    def test_export_feature_values(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.full_export.assert_called_with()
        self.id_matcher_mock.assert_called_with(ids=["*"])
        self.feature_selector_mock.assert_called_with(
            id_matcher = self.id_matcher_mock.return_value)

        self.io_pb2_mock.GcsDestination.assert_called_with(
            output_uri_prefix='gs://scratch-bucket/entity-type-id_d3d75')
        self.io_pb2_mock.CsvDestination.assert_called_with(
            gcs_destination=self.io_pb2_mock.GcsDestination.return_value)
        self.feature_value_destination_mock.assert_called_with(
            csv_destination=self.io_pb2_mock.CsvDestination.return_value)

        self.export_feature_values_request_mock.assert_called_with(
            full_export = self.full_export.return_value,
            entity_type = self.entity_type_mock.name,
            feature_selector = self.feature_selector_mock.return_value,
            destination = self.feature_value_destination_mock.return_value
        )

    def test_download_blob(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.storage_blob_mock.return_value.download_as_bytes.assert_called_with()
        self.storage_blob_content_mock.assert_called_with('utf-8')
        self.stringio_mock.assert_called_with(newline='')
        self.stringio_mock.return_value.__enter__().write.assert_called_with(self.storage_blob_content_mock.return_value)
        self.stringio_mock.return_value.__enter__().seek.assert_called_with(0)

    def test_parse_download(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.csv_mock.reader.assert_called_with(self.stringio_mock.return_value.__enter__.return_value)
        self.csv_mock.reader.return_value.__iter__.assert_called()

    def test_delete_blob(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.storage_blob_mock.return_value.delete.assert_called_with()

    def test_write_to_file(self):
        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', '.*')
        tool.execute()

        self.open_mock.assert_called_with('export.txt', 'w', newline='')
        self.csv_mock.writer.assert_called_with(self.open_mock.return_value.__enter__.return_value, delimiter='/')
        self.csv_mock.writer.return_value.writerow.assert_called_with(
            ['featurestores', 'featurestore-id', 'entityTypes',
             'entity-type-id', 'entities', 'entity-id', 'features',
             'feature-id', 'featureDataTypes', 'STRING', 'featureValues',
             'feature-value'])

    def test_filter_in_featurestores(self):
        self.featurestore_mock = absltest.mock.create_autospec(featurestore_pb2)
        self.featurestore_mock.name = 'projects/project-number/locations/region/featurestores/dog'

        self.fsclient_mock.return_value.list_featurestores.return_value = [self.featurestore_mock]

        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', 'd.*', '.*')
        tool.execute()

        self.fsclient_mock.return_value.list_entity_types.assert_called_with(
            parent=self.featurestore_mock.name)

    def test_filter_out_featurestores(self):
        self.featurestore_mock = absltest.mock.create_autospec(featurestore_pb2)
        self.featurestore_mock.name = 'projects/project-number/locations/region/featurestores/cat'

        self.fsclient_mock.return_value.list_featurestores.return_value = [self.featurestore_mock]

        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', 'd.*', '.*')
        tool.execute()

        self.fsclient_mock.return_value.list_entity_types.assert_not_called()

    def test_filter_in_entity_types(self):
        self.entity_type_mock = absltest.mock.create_autospec(entity_type_pb2)
        self.entity_type_mock.name = 'projects/project-number/locations/region/featurestores/featurestore-id/entityTypes/dog'

        self.fsclient_mock.return_value.list_entity_types.return_value = [self.entity_type_mock]

        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', 'd.*')
        tool.execute()

        self.fsclient_mock.return_value.list_features.assert_called_with(
            parent=self.entity_type_mock.name)

    def test_filter_out_entity_types(self):
        self.entity_type_mock = absltest.mock.create_autospec(entity_type_pb2)
        self.entity_type_mock.name = 'projects/project-number/locations/region/featurestores/featurestore-id/entityTypes/cat'

        self.fsclient_mock.return_value.list_entity_types.return_value = [self.entity_type_mock]

        tool = data_exporter.DataExporter('project-id', 'region',
            'gs://scratch-bucket/', 'export.txt', '.*', 'd.*')
        tool.execute()

        self.fsclient_mock.return_value.list_features.assert_not_called()

    def test_bad_gs_path(self):
        with self.assertRaises(ValueError):
            tool = data_exporter.DataExporter('project-id', 'region',
            '//scratch_space/some/path', 'export.txt', '.*', 'd.*')
            tool.execute()


if __name__ == '__main__':
    absltest.main()
