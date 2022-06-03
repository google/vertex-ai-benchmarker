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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.TextFormat;
import featurestoreloadtestframework.lib.FeaturestoreOnlineService.ReadFeatureValuesRequest;
import featurestoreloadtestframework.lib.FeaturestoreOnlineService.Request;
import featurestoreloadtestframework.lib.FeaturestoreOnlineService.Requests;
import featurestoreloadtestframework.lib.FeaturestoreOnlineService.RequestsPerFeaturestore;
import featurestoreloadtestframework.lib.FeaturestoreOnlineService.StreamingReadFeatureValuesRequest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.UUID;

public class FeaturestoreInputFilesParser {
  private String queryFilePath;
  private String entityFilePath = "";
  private String entityQuery = "";

  public FeaturestoreInputFilesParser(String queryFilePath) {
    this.queryFilePath = queryFilePath;
  }

  public FeaturestoreInputFilesParser(String queryFilePath, String entityFilePath) {
    this.queryFilePath = queryFilePath;
    this.entityFilePath = entityFilePath;
  }

  public FeaturestoreInputFilesParser(
      String queryFilePath, String entityQuery, boolean useBigQuery) {
    assert (useBigQuery);
    this.queryFilePath = queryFilePath;
    this.entityQuery = entityQuery;
  }

  protected String readContentsGCS(String filePath) {
    if (!queryFilePath.startsWith("gs://")) {
      throw new IllegalArgumentException("gcs path does not start with gs:/");
    }
    String[] split = filePath.replaceAll("gs://", "").split("/", 2);
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of(split[0], split[1]);
    Blob blob = storage.get(blobId);
    byte[] content = blob.getContent();
    return new String(content);
  }

  protected String readContentsLocal(String filePath) throws IOException {
    Scanner s = new Scanner(new File(filePath));
    String contents = "";
    while (s.hasNextLine()) {
      contents += s.nextLine() + "\n";
    }
    return contents;
  }

  protected void readContentsBigQuery(
      HashMap<String, HashMap<String, List<String>>> entityIds, String query)
      throws IOException, InterruptedException {
    // Code mostly from BQ example:
    // https://github.com/googleapis/java-bigquery/blob/HEAD/samples/snippets/src/main/java/com/example/bigquery/SimpleApp.java
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    TableResult result = queryJob.getQueryResults();

    for (FieldValueList row : result.iterateAll()) {
      String featurestore_id = row.get("featurestore_id").getStringValue();
      String entity_type_id = row.get("entity_type_id").getStringValue();
      String entity_id = row.get("entity_id").getStringValue();
      putEntityId(entityIds, featurestore_id, entity_type_id, entity_id);
    }
  }

  public List<FeatureStoreInput> contentsToFeaturestoreInput()
      throws IOException, InterruptedException {
    HashMap<String, HashMap<String, List<String>>> entityIds = parseEntityFile();
    return parseQueryFile(entityIds);
  }

  private List<FeatureStoreInput> parseQueryFile(
      HashMap<String, HashMap<String, List<String>>> allEntityIds) throws IOException {
    String contents =
        queryFilePath.startsWith("gs://")
            ? readContentsGCS(queryFilePath)
            : readContentsLocal(queryFilePath);
    List<FeatureStoreInput> featureStoreInputs = new ArrayList<>();
    Requests allRequests = TextFormat.parse(contents, Requests.class);
    for (RequestsPerFeaturestore requestsPerFeaturestore :
        allRequests.getRequestsPerFeaturestoreList()) {
      String featurestoreId = requestsPerFeaturestore.getFeaturestoreId();
      for (Request request : requestsPerFeaturestore.getRequestsList()) {
        if (request.hasReadFeatureValuesRequest()) {
          featureStoreInputs.addAll(
              readFeatureValuesRequestProtoToFeatureStoreInput(
                  allEntityIds, featurestoreId, request));
        } else if (request.hasStreamingReadFeatureValuesRequest()) {
          featureStoreInputs.add(
              streamingReadFeatureValuesRequestProtoToFeatureStoreInput(
                  allEntityIds, featurestoreId, request));
        }
      }
    }
    return featureStoreInputs;
  }

  private FeatureStoreInput streamingReadFeatureValuesRequestProtoToFeatureStoreInput(
      HashMap<String, HashMap<String, List<String>>> allEntityIds,
      String featurestoreId,
      Request request) {
    StreamingReadFeatureValuesRequest streamingReadFeatureValuesRequest =
        request.getStreamingReadFeatureValuesRequest();
    List<String> entityIds = new ArrayList<>();
    String entityType = streamingReadFeatureValuesRequest.getEntityType();
    List<String> matchingIds = getEntityIds(allEntityIds, featurestoreId, entityType);
    for (String entityId : streamingReadFeatureValuesRequest.getEntityIdsList()) {
      entityIds.addAll(processEntity(matchingIds, entityId));
    }
    List<String> featureIds = new ArrayList<>();
    for (String featureId :
        streamingReadFeatureValuesRequest.getFeatureSelector().getIdMatcher().getIdsList()) {
      featureIds.add(featureId);
    }
    return new FeatureStoreInput(featurestoreId, entityType, entityIds, featureIds);
  }

  private List<FeatureStoreInput> readFeatureValuesRequestProtoToFeatureStoreInput(
      HashMap<String, HashMap<String, List<String>>> allEntityIds,
      String featurestoreId,
      Request request) {
    List<FeatureStoreInput> featureStoreInputs = new ArrayList<>();
    ReadFeatureValuesRequest readFeatureValuesRequest = request.getReadFeatureValuesRequest();
    List<String> entityIds = new ArrayList<>();
    String entityType = readFeatureValuesRequest.getEntityType();
    List<String> matchingIds = getEntityIds(allEntityIds, featurestoreId, entityType);
    entityIds.addAll(processEntity(matchingIds, readFeatureValuesRequest.getEntityId()));
    List<String> featureIds = new ArrayList<>();
    for (String featureId :
        readFeatureValuesRequest.getFeatureSelector().getIdMatcher().getIdsList()) {
      featureIds.add(featureId);
    }
    for (String entityId : entityIds) {
      featureStoreInputs.add(
          new FeatureStoreInput(featurestoreId, entityType, entityId, featureIds));
    }
    return featureStoreInputs;
  }

  private List<String> processEntity(List<String> matchingIds, String entityId) {
    List<String> entityIds = new ArrayList<>();
    if (entityId.equals("${ENTITY_ID}")) {
      entityIds.addAll(matchingIds);
    } else {
      entityIds.add(entityId);
    }
    return entityIds;
  }

  private HashMap<String, HashMap<String, List<String>>> parseEntityFile()
      throws IOException, InterruptedException {
    HashMap<String, HashMap<String, List<String>>> entityIds = new HashMap<>();
    if (!this.entityQuery.isEmpty()) {
      readContentsBigQuery(entityIds, entityQuery);
    } else if (!this.entityFilePath.isEmpty()) {
      readContentsFile(entityIds);
    }
    return entityIds;
  }

  private void readContentsFile(HashMap<String, HashMap<String, List<String>>> entityIds)
      throws IOException {
    String entityContents =
        entityFilePath.startsWith("gs://")
            ? readContentsGCS(entityFilePath)
            : readContentsLocal(entityFilePath);
    StringTokenizer entityTokenizer = new StringTokenizer(entityContents);
    while (entityTokenizer.hasMoreTokens()) {
      String entityToken = entityTokenizer.nextToken();
      String[] entitySplit = entityToken.split("/");
      if (entitySplit.length != 6) {
        throw new IllegalArgumentException("Entity Resource ill-formatted.");
      } else {
        String featurestoreId = entitySplit[1];
        String entityTypeId = entitySplit[3];
        String entityId = entitySplit[5];
        putEntityId(entityIds, featurestoreId, entityTypeId, entityId);
      }
    }
  }

  private void putEntityId(
      HashMap<String, HashMap<String, List<String>>> entityIds,
      String featurestore,
      String entityType,
      String entityId) {
    if (!entityIds.containsKey(featurestore)) {
      entityIds.put(featurestore, new HashMap<>());
    }
    if (!entityIds.get(featurestore).containsKey(entityType)) {
      entityIds.get(featurestore).put(entityType, new ArrayList<>());
    }
    entityIds.get(featurestore).get(entityType).add(entityId);
  }

  private List<String> getEntityIds(
      HashMap<String, HashMap<String, List<String>>> entityIds,
      String featurestore,
      String entityType) {
    return entityIds
        .getOrDefault(featurestore, new HashMap<>())
        .getOrDefault(entityType, new ArrayList<>());
  }
}
