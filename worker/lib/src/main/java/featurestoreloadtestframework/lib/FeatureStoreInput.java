/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package featurestoreloadtestframework.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;


public class FeatureStoreInput {
    private String featureStoreID;
    private String entityType;
    private Optional<String> entityID;
    private List<String> entityIDs;
    private List<String> featureIDs;

    // If multiple locations are supported, inputs now also require location
    public FeatureStoreInput(String featureStoreID, String entityType, String entityID, List<String> featureIDs) {
        this.featureStoreID = featureStoreID;
        this.entityType = entityType;
        this.entityID = Optional.of(entityID);
        this.entityIDs = new ArrayList();
        this.featureIDs = featureIDs;
    }

    public FeatureStoreInput(String featureStoreID, String entityType, List<String> entityIDs, List<String> featureIDs) {
        this.featureStoreID = featureStoreID;
        this.entityType = entityType;
        this.entityIDs = entityIDs;
        this.featureIDs = featureIDs;
    }

    public String getFeatureStoreID() {
        return featureStoreID;
    }

    public String getEntityType() {
        return entityType;
    }

    public List<String> getFeatureIDs() {
        return featureIDs;
    }

    public Optional<String> getEntityID() {
        if (! entityID.isEmpty()) {
            return entityID;
        }
        return Optional.empty();
    }

    public List<String> getEntityIDs() {
        return entityIDs;
    }

    public String toString() {
        return String.format("featureStoreID: %s, entityType: %s, entityIDs: %s, featureIDs: %s",
            featureStoreID, entityType, entityID, featureIDs);
    }

}