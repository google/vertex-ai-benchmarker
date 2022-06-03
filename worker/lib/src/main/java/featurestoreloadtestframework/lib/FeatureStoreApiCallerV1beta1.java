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

import com.google.cloud.aiplatform.v1beta1.EntityType;
import com.google.cloud.aiplatform.v1beta1.EntityTypeName;
import com.google.cloud.aiplatform.v1beta1.FeaturestoreOnlineServingServiceClient;
import com.google.cloud.aiplatform.v1beta1.FeatureSelector;
import com.google.cloud.aiplatform.v1beta1.FeaturestoreOnlineServingServiceSettings;
import com.google.cloud.aiplatform.v1beta1.IdMatcher;
import com.google.cloud.aiplatform.v1beta1.LocationName;
import com.google.cloud.aiplatform.v1beta1.ReadFeatureValuesRequest;
import com.google.cloud.aiplatform.v1beta1.StreamingReadFeatureValuesRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

// Apply throughout, figure out FeatureStore (product?) vs Featurestore (an instance)
public class FeatureStoreApiCallerV1beta1 extends FeatureStoreApiCaller {
    private FeaturestoreOnlineServingServiceClient onlineClient;
    
    public FeatureStoreApiCallerV1beta1(
        String project, String location, FeatureStoreApiCaller.REST_METHOD method) {
        super(project, location, method);
        try {
            FeaturestoreOnlineServingServiceSettings onlineSettings = FeaturestoreOnlineServingServiceSettings.newBuilder(
                ).setEndpoint(endpoint).build();
            onlineClient = FeaturestoreOnlineServingServiceClient.create(onlineSettings);
        } catch (IOException e) {
            System.out.println(e.toString());
        }
    }
    
    public FeatureStoreApiCallerV1beta1(
        String project, String location, String endpointOverride,
        FeatureStoreApiCaller.REST_METHOD method) {
        super(project, location, endpointOverride, method);
        try {
            FeaturestoreOnlineServingServiceSettings onlineSettings = FeaturestoreOnlineServingServiceSettings.newBuilder(
                ).setEndpoint(endpoint).build();
            onlineClient = FeaturestoreOnlineServingServiceClient.create(onlineSettings);
        } catch (IOException e) {
            System.out.println(e.toString());
        }
    }

    public void call(FeatureStoreInput featureStoreInput) {
        if (featureStoreInput.getEntityIDs().size() > 0) {
            streamingReadFeaturesValuesCall(featureStoreInput);
        } else if (! featureStoreInput.getEntityID().isEmpty()) {
            readFeaturesValuesCall(featureStoreInput);
        } else {
            throw new IllegalArgumentException("Malformed FeatureStoreInput");
        }
    }

    private void readFeaturesValuesCall(FeatureStoreInput featureStoreInput) {
        ReadFeatureValuesRequest request = ReadFeatureValuesRequest.newBuilder()
           .setEntityType(
               EntityTypeName.of(project, location, featureStoreInput.getFeatureStoreID(),
                                    featureStoreInput.getEntityType())
                   .toString())
           .setEntityId(featureStoreInput.getEntityID().get())
           .setFeatureSelector(FeatureSelector.newBuilder().setIdMatcher(IdMatcher.newBuilder().addAllIds(featureStoreInput.getFeatureIDs()).build()).build())
           .build();
        onlineClient.readFeatureValues(request);
    }

    private void streamingReadFeaturesValuesCall(FeatureStoreInput featureStoreInput) {
        StreamingReadFeatureValuesRequest request = StreamingReadFeatureValuesRequest.newBuilder()
           .setEntityType(
               EntityTypeName.of(project, location, featureStoreInput.getFeatureStoreID(),
                                    featureStoreInput.getEntityType())
                   .toString())
           .addAllEntityIds(featureStoreInput.getEntityIDs())
           .setFeatureSelector(FeatureSelector.newBuilder().setIdMatcher(IdMatcher.newBuilder().addAllIds(featureStoreInput.getFeatureIDs()).build()).build())
           .build();
        onlineClient.streamingReadFeatureValuesCallable().call(request);
    }

}