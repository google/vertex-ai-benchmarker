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

from collections import defaultdict
import csv
from io import StringIO
from os import path
import re
from uuid import uuid4

from absl import logging
from google.cloud import storage
from google.cloud.aiplatform_v1 import FeaturestoreServiceClient
from google.cloud.aiplatform_v1.types import entity_type as entity_type_pb2
from google.cloud.aiplatform_v1.types import FeatureSelector, IdMatcher
from google.cloud.aiplatform_v1.types import featurestore as featurestore_pb2
from google.cloud.aiplatform_v1.types import featurestore_service as featurestore_service_pb2
from google.cloud.aiplatform_v1.types import io as io_pb2
from lib.featurestore_data_classes import EntityType
from lib.featurestore_data_classes import Featurestore

GS_ORIGINAL_PATH_FORMAT = '^gs://(?P<bucket_name>[^/]+)/?(?P<blob_name>.*)$'
GS_FINAL_PATH_FORMAT = '^gs://(?P<bucket_name>[^/]+)/(?P<blob_name>.+)$'


class DataExporter:
  """The data exporter exports data from an existing FeatureStore instance into a flat data file.
  """

  def __init__(self, project_id: str, region: str, gcs_path: str,
               export_file_path: str, featurestore_id_regex: str,
               entity_type_id_regex: str):
    """Inititializes a data importer.

        Args:
          project_id: ID of a project.
          region: Region to upload the data into.
          gcs_path: Storage path (temporarily used to export data from the
            featurestores); e.g. gs://scratch_bucket/.
          export_file_path: Export file path for the generated flat data file.
          featurestore_id_regex: Export only Featurestores that match this
            regex.
          entity_type_regex: Export only EntityTypes that match this regex.
    """
    if not re.match(GS_ORIGINAL_PATH_FORMAT, gcs_path):
      raise ValueError(f'Not a valid GS path format: {gcs_path}')
    self._project_id = project_id
    self._region = region
    self._export_file_path = export_file_path
    self._gcs_path = gcs_path
    self._featurestore_id_regex = featurestore_id_regex
    self._entity_type_id_regex = entity_type_id_regex
    self._api_endpoint = f'{region}-aiplatform.googleapis.com'

  def execute(self):
    'Execute the data exporter.'
    featurestores = self._export_featurestores()
    self._write_to_file(featurestores)

  def _export_featurestores(self) -> list[Featurestore]:
    """Export featurestores.

        Returns:
            Featurestore data for all the Featurestores and entity types
                that match the regexes.
    """
    featurestore_client = FeaturestoreServiceClient(
        client_options={'api_endpoint': self._api_endpoint})
    common_location_path = featurestore_client.common_location_path(
        self._project_id, self._region)

    featurestore_objects = featurestore_client.list_featurestores(
        parent=common_location_path)
    featurestores_data = defaultdict(Featurestore)

    for featurestore_object in featurestore_objects:
      featurestore_id = featurestore_object.name.split('/')[-1]
      if not re.match(self._featurestore_id_regex, featurestore_id):
        continue
      self._export_featurestore_data(featurestore_client, featurestores_data,
                                     featurestore_object, featurestore_id)

    return featurestores_data

  def _export_featurestore_data(self,
                                featurestore_client: FeaturestoreServiceClient,
                                featurestores_data: list[Featurestore],
                                featurestore_object: featurestore_pb2,
                                featurestore_id: str):
    """Export the data from a Featurestore.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              export from.
            featurestores_data: Local Featurestores data.
            featurestore_object: Remote Featurestore object.
            featurestore_id: ID of the Featurestore.
    """
    logging.info(f"Opening Featurestore '{featurestore_id}'...")
    featurestore_data = featurestores_data[featurestore_id]
    for entity_type in featurestore_client.list_entity_types(
        parent=featurestore_object.name):
      entity_type_id = entity_type.name.split('/')[-1]
      if not re.match(self._entity_type_id_regex, entity_type_id):
        continue
      self._export_entity_type_data(featurestore_client, entity_type_id,
                                    featurestore_data, entity_type)

  def _export_entity_type_data(self,
                               featurestore_client: FeaturestoreServiceClient,
                               entity_type_id: str,
                               featurestore_data: Featurestore,
                               entity_type_object: entity_type_pb2):
    """Export the data for an entity type.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              export from.
            entity_type_id: ID of the entity type.
            featurestore_data: Local Featurestore data.
            entity_type_object: Remote entity type object.
    """
    logging.info(f"Opening entity type '{entity_type_id}'...")
    entity_type_data = featurestore_data.entity_types[entity_type_id]

    self._export_feature_metadata(featurestore_client, entity_type_id,
                                  entity_type_data, entity_type_object)
    self._export_entity_type_entity_data(featurestore_client, entity_type_id,
                                         entity_type_data, entity_type_object)

  def _export_feature_metadata(self,
                               featurestore_client: FeaturestoreServiceClient,
                               entity_type_id: str,
                               entity_type_data: EntityType,
                               entity_type_object: entity_type_pb2):
    """Export the feature metadata for an entity type.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              export from.
            entity_type_id: ID of the entity type.
            entity_type_data: Local entity type data.
            entity_type_object: Remote entity type object.
    """
    logging.info(
        f"Exporting feature metadata for entity type '{entity_type_id}'...")
    for feature in featurestore_client.list_features(
        parent=entity_type_object.name):
      feature_id = feature.name.split('/')[-1]
      entity_type_data.features_metadata[
          feature_id].data_type = feature.value_type

  def _export_entity_type_entity_data(
      self, featurestore_client: FeaturestoreServiceClient, entity_type_id: str,
      entity_type_data: EntityType, entity_type_object: entity_type_pb2):
    """Export the entity data for an entity type.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              export from.
            entity_type_id: ID of the entity type.
            entity_type_data: Local entity type data.
            entity_type_object: Remote entity type object.
    """
    logging.info(
        f"Exporting entity data from entity type '{entity_type_id}'...")
    unique_folder_path, blob = self._locate_folder_and_blob(entity_type_id)
    try:
      with StringIO(newline='') as csv_file:
        self._export_entity_and_features(featurestore_client,
                                         entity_type_object, unique_folder_path,
                                         blob, csv_file)
        self._process_entities_and_features(entity_type_id, entity_type_data,
                                            csv_file)
    finally:
      blob.delete()

  def _write_to_file(self, featurestores_data: list[Featurestore]):
    """Write data for Featurestore(s) to the export file.

        Args:
            featurestores_data: Data from Featurestore(s).
    """
    logging.info(f"Writing file '{self._export_file_path}'...")
    with open(self._export_file_path, 'w', newline='') as file:
      data_writer = csv.writer(file, delimiter='/')
      for featurestore_id, featurestore in featurestores_data.items():
        for entity_type_id, entity_type in featurestore.entity_types.items():
          for entity_id, entity in entity_type.entities.items():
            for feature_id, feature in entity.features.items():
              line_data = [
                  'featurestores', featurestore_id, 'entityTypes',
                  entity_type_id, 'entities', entity_id, 'features', feature_id,
                  'featureDataTypes',
                  entity_type.features_metadata[feature_id].data_type.name,
                  'featureValues', feature.value
              ]
              data_writer.writerow(line_data)

  def _locate_folder_and_blob(self,
                              entity_type_id: str) -> tuple[str, storage.Blob]:
    """Locate the folder to write export data in and the corresponding blob object.

        Args:
            entity_type_id: Entitiy type ID for the data in the export file.

        Returns:
            Unique folder path for the entity type data.
            Blob object for the data file.
    """
    # Create a unique file path for each entity type
    unique_folder = f'{entity_type_id}_{str(uuid4())[:5]}'
    unique_folder_path = path.join(self._gcs_path, unique_folder)
    filepath = path.join(
        unique_folder_path,
        '000000000000.csv')  # 12 zeroes, the FS client always uses this name

    # Locate the blob object (nothing is uploaded yet)
    match = re.match(GS_FINAL_PATH_FORMAT, filepath)
    gcs_client = storage.Client(self._project_id)
    bucket = gcs_client.bucket(match.group('bucket_name'))
    blob = bucket.blob(match.group('blob_name'))

    return unique_folder_path, blob

  def _export_entity_and_features(
      self, featurestore_client: FeaturestoreServiceClient,
      entity_type_object: entity_type_pb2, unique_folder_path: str,
      blob: storage.Blob, csv_file: StringIO):
    """Export entity and feature data values for entity type.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              export from.
            entity_type_object: Remote entity type object.
            unique_folder_path: Unique folder path for the entity type data.
            blob: Blob object for the data file.
            csv_file: String buffer representing the data in CSV format.
    """
    export_request = featurestore_service_pb2.ExportFeatureValuesRequest(
        full_export=featurestore_service_pb2.ExportFeatureValuesRequest
        .FullExport(),
        entity_type=entity_type_object.name,
        feature_selector=FeatureSelector(id_matcher=IdMatcher(ids=['*'])),
        destination=featurestore_service_pb2.FeatureValueDestination(
            csv_destination=io_pb2.CsvDestination(
                gcs_destination=io_pb2.GcsDestination(
                    output_uri_prefix=unique_folder_path))))
    featurestore_client.export_feature_values(request=export_request).result()
    entity_type_data_export = blob.download_as_bytes().decode('utf-8')
    csv_file.write(entity_type_data_export)
    csv_file.seek(0)

  def _process_entities_and_features(self, entity_type_id: str,
                                     entity_type_data: EntityType,
                                     csv_file: StringIO):
    """Process entity and feature data for an entity type.

        Args:
            entity_type_id: ID of the entity type.
            entity_type_data: Local entity type data.
            csv_file: String buffer representing the data in CSV format.
    """
    first_line = True
    features_in_order = list()

    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
      if first_line:
        # Header line
        first_line = False
        features_in_order = row
      else:
        self._process_data_line(entity_type_id, entity_type_data,
                                features_in_order, row)

  def _process_data_line(self, entity_type_id: str, entity_type_data: str,
                         features_in_order: list[str],
                         feature_values: list[str]):
    """Process one row of data belonging to one entity.

        Args:
            entity_type_id: ID of the entity type.
            entity_type_data: Local entity type data.
            features_in_order: List of the features, in the order they are in
              the line.
            fature_values: List of feature values for one entity.
    """
    entity_id = ''
    for feature_name, feature_value in zip(features_in_order, feature_values):
      if feature_name == f'entity_type_{entity_type_id}':
        entity_id = feature_value
      elif feature_name == 'feature_timestamp':
        # TODO: Support exporting feature timestamp?
        continue
      else:
        entity_type_data.entities[entity_id].features[
            feature_name].value = feature_value
