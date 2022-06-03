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
"""Moves data from a flat data file into a featurestore."""

from absl import app
from absl import flags
from lib.data_importer import DataImporter

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'project_id',
    None,
    'ID of the Google Cloud project you want to import data into.',
    required=True,
)
flags.DEFINE_string(
    'region',
    None,
    'Region you want to upload the data into.',
    required=True,
)
flags.DEFINE_string(
    'gcs_path',
    None,
    'Google Cloud Storage location in the same region '
    '(temporarily used to load data into the featurestores); e.g. gs://scratch_bucket/.',
    required=True,
)
flags.DEFINE_string(
    'import_file_path',
    'data/featurestore.txt',
    'Path to the file containing the data you want to import.',
)


def main(argv):
  del argv  # unused

  try:
    tool = DataImporter(
        FLAGS.project_id,
        FLAGS.region,
        FLAGS.import_file_path,
        FLAGS.gcs_path,
    )
    tool.execute()
  except Exception as ex:
    raise Exception(f'Failed to process file: {FLAGS.import_file_path}') from ex


if __name__ == '__main__':
  app.run(main)
