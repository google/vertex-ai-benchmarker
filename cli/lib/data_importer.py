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
from typing import Any
from uuid import uuid4

from absl import logging
from google.cloud import storage
from google.cloud.aiplatform_v1 import FeaturestoreServiceClient
from google.cloud.aiplatform_v1.types import entity_type as entity_type_pb2
from google.cloud.aiplatform_v1.types import feature as feature_pb2
from google.cloud.aiplatform_v1.types import featurestore as featurestore_pb2
from google.cloud.aiplatform_v1.types import featurestore_service as featurestore_service_pb2
from google.cloud.aiplatform_v1.types import io as io_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from lib.featurestore_data_classes import Entity
from lib.featurestore_data_classes import EntityType
from lib.featurestore_data_classes import Featurestore

GS_ORIGINAL_PATH_FORMAT = '^gs://(?P<bucket_name>[^/]+)/?(?P<blob_name>.*)$'
GS_FINAL_PATH_FORMAT = '^gs://(?P<bucket_name>[^/]+)/(?P<blob_name>.+)$'


class DataImporter:
  """The data importer moves data from an existing flat data file into a FeatureStore instance.
  """

  def __init__(self, project_id: str, region: str, import_file_path: str,
               gcs_path: str):
    """Inititializes a data importer.

        Args:
          project_id: ID of a project.
          region: Region to upload the data into.
          import_file_path: Import file path of a flat data file.
          gcs_path: Storage path (temporarily used to load data into the
            featurestores); e.g. gs://scratch_bucket/.
    """
    if not re.match(GS_ORIGINAL_PATH_FORMAT, gcs_path):
      raise ValueError(f'Not a valid GS path format: {gcs_path}')
    self._project_id = project_id
    self._region = region
    self._import_file_path = import_file_path
    self._gcs_path = gcs_path
    self._api_endpoint = f'{region}-aiplatform.googleapis.com'

  def _upload_data(self, featurestore_client: FeaturestoreServiceClient,
                   featurestores: list[Featurestore]) -> None:
    """Upload data to FeatureStore instance.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              upload to.
            featurestores: Featurestores with data to upload.
    """
    for featurestore_id, featurestore in featurestores.items():
      for entity_type_id, entity_type in featurestore.entity_types.items():
        logging.info(f"Uploading data for entity type '{entity_type_id}'...")
        filepath, blob = self._create_blob_object(entity_type_id)
        try:
          entity_id_column_id = self._upload_data_file(entity_type, blob)
          self._run_data_ingestion_job(featurestore_client, featurestore_id,
                                       entity_type_id, entity_type, filepath,
                                       entity_id_column_id)
        finally:
          blob.delete()

  def _run_data_ingestion_job(self,
                              featurestore_client: FeaturestoreServiceClient,
                              featurestore_id: str, entity_type_id: str,
                              entity_type: EntityType, filepath: str,
                              entity_id_column_id: str) -> None:
    """Run a data ingestion job for an entity type to upload data to a FeatureStore instance.

        Args:
          featurestore_client: FeatureStore service client for FS instance to
            upload to.
          featurestore_id: Featurestore to run the ingestion job against.
          entity_type_id: Entity type ID of data being uploaded.
          entity_type: Entity type object of data being uploaded.
          filepath: File with CSV of data to upload.
          entity_id_column_id: ID of the column containing the row's entity ID.
    """
    current_time = Timestamp()
    current_time.GetCurrentTime()
    # Can't be higher resolution than milliseconds
    current_time.nanos = (current_time.nanos // 1000000) * 1000000

    featurestore_client.import_feature_values(
        featurestore_service_pb2.ImportFeatureValuesRequest(
            entity_type=featurestore_client.entity_type_path(
                self._project_id, self._region, featurestore_id,
                entity_type_id),
            csv_source=io_pb2.CsvSource(
                gcs_source=io_pb2.GcsSource(uris=[filepath])),
            disable_online_serving=True,
            entity_id_field=entity_id_column_id,
            feature_specs=[
                featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(
                    id=x) for x in entity_type.features_metadata
            ],
            feature_time=current_time,
            worker_count=1,
            disable_ingestion_analysis=True,
        )).result()

  def _upload_data_file(self, entity_type: str, blob: storage.Blob) -> str:
    """Upload CSV data file to GCS.

        Args:
          entity_type: Entity type of data in the file.
          blob: GCS blob to upload into.

        Returns:
          ID of the column containing the row's entity ID.
    """
    with StringIO(newline='') as csv_file:
      csv_writer = csv.writer(csv_file)
      entity_id_column_id = self._write_header(entity_type, csv_writer)
      for entity_id, entity in entity_type.entities.items():
        self._write_row(entity_type, csv_writer, entity_id, entity)
      blob.upload_from_file(csv_file, rewind=True, content_type='text/csv')
      return entity_id_column_id

  def _write_row(self, entity_type: EntityType, csv_writer: Any, entity_id: str,
                 entity: Entity) -> None:
    """Write a row (1 entity) to the CSV file.

        Args:
          entity_type: Entity type object for the data.
          csv_writer: Writer for the CSV file.
          entity_id: ID of the entity.
          entity: Entity object.
    """
    entity_data = [entity_id]
    for feature_metadata_id in entity_type.features_metadata:
      entity_data.append(entity.features[feature_metadata_id].value)
    csv_writer.writerow(entity_data)

  def _write_header(self, entity_type: EntityType, csv_writer: Any) -> str:
    """Write the header of the CSV file.

        Args:
          entity_type: Entity type object for this file.
          csv_writer: Writer for the CSV file.

        Returns:
          ID of the column containing the row's entity ID.
    """
    entity_id_column_id = f'entity_id_{str(uuid4())[:5]}'
    header_data = [entity_id_column_id]
    header_data.extend(entity_type.features_metadata.keys())
    csv_writer.writerow(header_data)
    return entity_id_column_id

  def _create_blob_object(self,
                          entity_type_id: str) -> tuple[str, storage.Blob]:
    """Create a GCS blob object for an entity type's data.

        Args:
          entity_type_id: ID of the entity type to create the blob for.

        Returns:
          Filepath to blob.
          Created blob object.
    """
    filename = f'{entity_type_id}_{str(uuid4())[:5]}.csv'  # uuid to ensure uniqueness
    filepath = path.join(self._gcs_path, filename)
    gcs_client = storage.Client(self._project_id)
    match = re.match(GS_FINAL_PATH_FORMAT, filepath)
    bucket = gcs_client.bucket(match.group('bucket_name'))
    blob = bucket.blob(match.group('blob_name'))
    return filepath, blob

  def _parse_schema_and_data(self) -> list[Featurestore]:
    """Parse the input file to get the schema and data for the featurestores.

        Returns:
          List of featurestores.
    """
    featurestores = defaultdict(Featurestore)
    # Featurestore name's can collide with existing and recently deleted featurestores
    # So map them to a more unique name
    featurestore_id_map = {}

    with open(self._import_file_path, newline='') as file:
      logging.info(f"Parsing file '{self._import_file_path}'...")
      data_reader = csv.reader(file, delimiter='/')
      for row in data_reader:
        success, featurestore_id, entity_type_id, entity_id, feature_id, \
            feature_data_type, feature_value = self._parse_values(row)
        if success:
          self._store_values(featurestores, featurestore_id_map,
                             featurestore_id, entity_type_id, entity_id,
                             feature_id, feature_data_type, feature_value)
        else:
          logging.warning('Failed to parse line: %s', '/'.join(row))
      return featurestores

  def _store_values(self, featurestores: list[Featurestore],
                    featurestore_id_map: str, featurestore_id: str,
                    entity_type_id: str, entity_id: str, feature_id: str,
                    feature_data_type: str, feature_value: str) -> None:
    """Stores the values from a parsed line of the input file.

        Args:
            featurestores: featurestore data structure to store data in.
            featurestore_id_map: Map between the file's featurestore names and
              the generated unique names.
            featurestore_id: ID of featurestore.
            entity_type_id: ID of entity type.
            entity_id: ID of entity.
            feature_id: ID of feature.
            feature_data_type: Data type of feature (may be blank if already
              known).
            feature_value: Value of feature.
    """
    featurestore = self._map_featurestore(featurestores, featurestore_id_map,
                                          featurestore_id)
    entity_type = featurestore.entity_types[entity_type_id]
    success = self._set_feature_metadata(feature_id, feature_data_type,
                                         entity_type)
    # TODO: Add support for entities with multiple IDs?
    if success:
      entity_type.entities[entity_id].features[feature_id].value = feature_value

  def _set_feature_metadata(self, feature_id: str, feature_data_type: str,
                            entity_type: EntityType) -> bool:
    """Sets the feature metadata if it isn't already known.

        Args:
            feature_id: ID of the feature.
            feature_data_type: Data type of feature (may be blank if already
              known).
            entity_type: Entity type of feature.

        Returns:
            Whether setting the metadata was successful.
    """
    if feature_id not in entity_type.features_metadata:
      feature_data_type = feature_data_type.upper()
      feature_data_type_value = feature_pb2.Feature.ValueType.VALUE_TYPE_UNSPECIFIED
      try:
        feature_data_type_value = feature_pb2.Feature.ValueType[
            feature_data_type]
        # TODO: Add support for array values?
        if feature_data_type_value in (
            feature_pb2.Feature.ValueType.BOOL_ARRAY,
            feature_pb2.Feature.ValueType.DOUBLE_ARRAY,
            feature_pb2.Feature.ValueType.INT64_ARRAY,
            feature_pb2.Feature.ValueType.STRING_ARRAY):
          logging.warning(
              'Uploading array value types not currently supported.')
          return False
      except KeyError:
        logging.warning('Unknown datatype: %s', feature_data_type)
        return False
      feature = entity_type.features_metadata[feature_id]
      feature.data_type = feature_data_type_value
    return True

  def _map_featurestore(self, featurestores: list[Featurestore],
                        featurestore_id_map: dict[str, str],
                        featurestore_id: str) -> Featurestore:
    """Map featurstore names to a more unique name.

        Args:
            featurestores: List of featurestores to map.
            featurestore_id_map: Dictionary of existing mappings.
            reaturestore_id: Original ID of featurestore that needs mapped.

        Returns:
            Mapped featurestore.
    """
    if featurestore_id not in featurestore_id_map:
      featurestore_id_map[
          featurestore_id] = f'{featurestore_id}_{str(uuid4())[:5]}'
    featurestore_id = featurestore_id_map[featurestore_id]
    return featurestores[featurestore_id]

  def _parse_values(
      self, row: list[str]) -> tuple[bool, str, str, str, str, str, str]:
    """Parse the values on a line of the flat data file.

        Args:
            line: Line of text.

        Returns:
            Whether the line could be successfully parsed.
            ID of the featurestore.
            ID of the entity type.
            ID of the entity.
            ID of the feature.
            Data type of the feature.
            Value of the feature for the entity.
    """
    if len(row) != 12:
      return False, '', '', '', '', '', ''

    row = [row[1], row[3], row[5], row[7], row[9], row[11]]

    featurestore_id, entity_type_id, entity_id, feature_id, feature_data_type, \
        feature_value = row
    return True, featurestore_id, entity_type_id, entity_id, feature_id, \
        feature_data_type, feature_value

  def _create_schema(self, featurestore_client: FeaturestoreServiceClient,
                     featurestores: list[Featurestore]) -> None:
    """Create featurestore schema.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              upload to.
            festurestores: Featurestores whose schema to create.
    """
    common_location_path = featurestore_client.common_location_path(
        self._project_id, self._region)
    for featurestore_id, featurestore in featurestores.items():
      self._create_featurestore(featurestore_client, common_location_path,
                                featurestore_id)

      for entity_type_id, entity_type in featurestore.entity_types.items():
        featurestore_path = featurestore_client.featurestore_path(
            self._project_id, self._region, featurestore_id)
        self._create_entity_type(featurestore_client, featurestore_path,
                                 entity_type_id)

        entity_type_path = featurestore_client.entity_type_path(
            self._project_id, self._region, featurestore_id, entity_type_id)
        self._create_features(featurestore_client, entity_type_path,
                              entity_type)

  def _create_features(self, featurestore_client: FeaturestoreServiceClient,
                       entity_type_path: str, entity_type: EntityType) -> None:
    """Create features for an entity type.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              upload to.
            entity_type_path: Path to the entity type.
            entity_type: Entity type whose features to create.
    """
    feature_requests = [
        featurestore_service_pb2.CreateFeatureRequest(
            feature=feature_pb2.Feature(
                value_type=feature_metadata.data_type,
                description='',
                disable_monitoring=True,
            ),
            feature_id=feature_metadata_id)
        for (feature_metadata_id,
             feature_metadata) in entity_type.features_metadata.items()
    ]
    feature_names = "', '".join(entity_type.features_metadata.keys())

    logging.info(f"Creating feature(s) '{feature_names}'...")
    featurestore_client.batch_create_features(
        parent=entity_type_path, requests=feature_requests).result()

  def _create_entity_type(self, featurestore_client: FeaturestoreServiceClient,
                          featurestore_path: str, entity_type_id: str) -> None:
    """Create an entity type for a featurestore.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              upload to.
            featurestore_path: Path to the featurestore.
            entity_type: Entity type to create.
    """
    logging.info(f"Creating entity type '{entity_type_id}'...")
    featurestore_client.create_entity_type(
        featurestore_service_pb2.CreateEntityTypeRequest(
            parent=featurestore_path,
            entity_type_id=entity_type_id,
            entity_type=entity_type_pb2.EntityType(description=''))).result()

  def _create_featurestore(self, featurestore_client: FeaturestoreServiceClient,
                           base_resource_path: str,
                           featurestore_id: str) -> None:
    """Create a featurestore.

        Args:
            featurestore_client: FeatureStore service client for FS instance to
              upload to.
            base_resource_path: Path to the base resource.
            featurestore_id: ID of the featurestore to create.
    """
    logging.info(f"Creating featurestore '{featurestore_id}'...")
    logging.info(f'This may take 30+ minutes...')
    featurestore_client.create_featurestore(
        featurestore_service_pb2.CreateFeaturestoreRequest(
            parent=base_resource_path,
            featurestore_id=featurestore_id,
            featurestore=featurestore_pb2.Featurestore(
                online_serving_config=featurestore_pb2.Featurestore
                .OnlineServingConfig(fixed_node_count=1)))).result()

  def execute(self) -> None:
    'Execute the data import.'
    featurestores = self._parse_schema_and_data()
    if featurestores:
      featurestore_client = FeaturestoreServiceClient(
          client_options={'api_endpoint': self._api_endpoint})
      self._create_schema(featurestore_client, featurestores)
      self._upload_data(featurestore_client, featurestores)
