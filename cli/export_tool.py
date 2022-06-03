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
"""Exports data from a Feature Store instance to a flat data file."""

from absl import app
from absl import flags
from lib.data_exporter import DataExporter

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'project_id',
    None,
    'ID of the Google Cloud project you want to export data from.',
    required=True,
)
flags.DEFINE_string(
    'region',
    None,
    'Region you want to export the data from.',
    required=True,
)
flags.DEFINE_string(
    'gcs_path',
    None,
    'Google Cloud Storage location in the same region '
    '(temporarily used to export data from the featurestores); e.g. gs://scratch_bucket/.',
    required=True,
)
flags.DEFINE_string(
    'export_file_path',
    'export.txt',
    'Path to where the data should be exported',
)
flags.DEFINE_string(
    'featurestore_id_regex',
    '.*',
    'Export only Featurestores that match this regex',
)
flags.DEFINE_string(
    'entity_type_id_regex',
    '.*',
    'Export only entity types that match this regex',
)


def main(argv):
  del argv  # unused

  try:
    tool = DataExporter(
        FLAGS.project_id,
        FLAGS.region,
        FLAGS.gcs_path,
        FLAGS.export_file_path,
        FLAGS.featurestore_id_regex,
        FLAGS.entity_type_id_regex,
    )
    tool.execute()
  except Exception as ex:
    raise Exception(
        f'Failed to export Featurestore data: {FLAGS.export_file_path}') from ex


if __name__ == '__main__':
  app.run(main)
