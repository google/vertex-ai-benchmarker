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
from dataclasses import dataclass, field
from typing import Any, DefaultDict

from google.cloud.aiplatform_v1.types import feature as feature_pb2

@dataclass()
class FeatureMetadata:
    data_type: feature_pb2.Feature.ValueType = feature_pb2.Feature.ValueType.VALUE_TYPE_UNSPECIFIED


@dataclass()
class Feature:
    value: Any = ''


@dataclass()
class Entity:
    features: DefaultDict[str, Feature] = field(
        default_factory=lambda: defaultdict(Feature))


@dataclass
class EntityType:
    features_metadata: DefaultDict[str, FeatureMetadata] = field(
        default_factory=lambda: defaultdict(FeatureMetadata))
    entities: DefaultDict[str, Entity] = field(
        default_factory=lambda: defaultdict(Entity))


@dataclass
class Featurestore:
    entity_types: DefaultDict[str, EntityType] = field(
        default_factory=lambda: defaultdict(EntityType))
