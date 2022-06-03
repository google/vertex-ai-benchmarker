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

import java.util.Arrays;
import java.lang.IllegalArgumentException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageException;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;

import com.google.cloud.spanner.Backup;
import com.google.cloud.spanner.BackupId;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.RestoreInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerBatchUpdateException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.api.gax.longrunning.OperationFuture;


import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ChartWriter {
    private Storage storageClient;
    private String projectId;
    private String location;
    private StorageClass storageClass;
    private BigQuery bigquery;
    private Dataset chartingDataset;
    private SpannerOptions options;
    private Spanner spanner;
    private DatabaseId db;
    private DatabaseClient dbClient;
    private DatabaseAdminClient dbAdminClient;
    private InstanceAdminClient instanceAdminClient;

    public ChartWriter(String projectId, String location) {
        this.projectId = projectId;
        this.location = location;
        if (!this.projectId.equals("")) {
            this.storageClient = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
            this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        } else {
            this.bigquery = BigQueryOptions.getDefaultInstance().getService();
            this.storageClient = StorageOptions.getDefaultInstance().getService();
        }
        this.storageClass = StorageClass.STANDARD;
    }

    public ChartWriter(String projectId, String instance, String database) {
        this.createSpannerClient(projectId, instance, database);
    }

    private void createSpannerClient(String project, String instance, String database) throws IllegalArgumentException {
        this.options = SpannerOptions.newBuilder().build();
        this.spanner = options.getService();
        try {
            this.db = DatabaseId.of(project, instance, database);
            // This will return the default project id based on the environment.
            String clientProject = spanner.getOptions().getProjectId();
            if (!db.getInstanceId().getProject().equals(clientProject)) {
                throw new IllegalArgumentException("Invalid project specified. Project in the database id should match the"
                    + "project name set in the environment variable GOOGLE_CLOUD_PROJECT. Expected: "
                    + clientProject);
            }

            // [START init_client]
            this.dbClient = spanner.getDatabaseClient(db);
            this.dbAdminClient = spanner.getDatabaseAdminClient();
            this.instanceAdminClient = spanner.getInstanceAdminClient();
        } finally {
            this.spanner.close();
        }
        // [END init_client]
        System.out.println("Closed client");
    }

    private Bucket createBucketIfNotExist(String bucketName) throws StorageException {
        Bucket bucket = storageClient.get(bucketName);
        if (bucket == null) {
            if (this.projectId.equals("")) {
                throw new StorageException(404, String.format("Unable to find bucket `%s`", bucketName));
            }
            try {
                BucketInfo.Builder bucketInfo = BucketInfo.newBuilder(bucketName);
                if (this.storageClass != null) {
                    bucketInfo = bucketInfo.setStorageClass(storageClass);
                }
                if (!this.location.equals("")) {
                    bucketInfo = bucketInfo.setLocation(location);
                }
                bucket =
                    this.storageClient.create(
                        bucketInfo.build()
                        );
            } catch (StorageException e) {
                System.err.println("Failed to create GCS bucket: " + e.toString());
                throw e;
            }
		}

        return bucket;
    }

    public Blob write(String bucketName, String blobPath, String blobContent) {
        Bucket bucket = this.createBucketIfNotExist(bucketName);
        Blob b = bucket.create(blobPath, String.join(
			"\n", blobContent).getBytes(), "text/plain");
        return b;
    }

    public void createDatasetIfNotExist(String datasetName) throws BigQueryException {
        try {
            if (!this.projectId.equals("")) {
                Page<Dataset> datasets = this.bigquery.listDatasets(this.projectId, DatasetListOption.pageSize(100));
                if (datasets == null) {
                    System.out.println("No datasests found in project " + this.projectId);
                    return;
                }
                HashMap<String, Dataset> existingDatasets = new HashMap<>();
                datasets
                    .iterateAll()
                    .forEach(
                        dataset -> existingDatasets.put(dataset.getDatasetId().getDataset(), dataset));

                if (!existingDatasets.containsKey(datasetName)) {
                    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

                    this.chartingDataset = this.bigquery.create(datasetInfo);
                    String newDatasetName = this.chartingDataset.getDatasetId().getDataset();
                    System.out.println(newDatasetName + " created successfully");
                } else {
                    this.chartingDataset = existingDatasets.get(datasetName);
                }
            } else {
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

                this.chartingDataset = this.bigquery.create(datasetInfo);
                String newDatasetName = this.chartingDataset.getDatasetId().getDataset();
                System.out.println(newDatasetName + " created successfully");
            }
          } catch (BigQueryException e) {
            System.err.println("Dataset was not created. \n" + e.toString());
            throw e;
          }
    }

    public void exportToBQ(String datasetName, String tableName, String sourceUri, WriteDisposition option) throws InterruptedException, BigQueryException {
        Schema schema =
			Schema.of(
				Field.of("start_time", StandardSQLTypeName.TIMESTAMP),
				Field.of("latency", StandardSQLTypeName.TIME));
        this.createDatasetIfNotExist(datasetName);
        this.gcsToBQ(datasetName, tableName, sourceUri, schema, option);
    }

    private void gcsToBQ(String datasetName, String tableName, String sourceUri, Schema schema, WriteDisposition option) throws InterruptedException {
        // Skip header row in the file.
        CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
        TableId tableId = TableId.of(datasetName, tableName);
        LoadJobConfiguration loadConfig =
            LoadJobConfiguration.newBuilder(tableId, sourceUri, csvOptions)
            .setWriteDisposition(option)
            .setSchema(schema)
            .build();

        // Load data from a GCS CSV file into the table
        Job job = bigquery.create(JobInfo.of(loadConfig));
        // Blocks until this load table job completes its execution, either failing or succeeding.
        try {
            job = job.waitFor();
        } catch (InterruptedException e) {
            System.err.println("Error when calling job.WaitFor(): " + e.toString());
            throw e;
        }
        

        if (job.isDone()) {
            System.out.println("CSV from GCS successfully added during load append job");
        } else {
            System.out.println(
                "BigQuery was unable to load into the table due to an error:"
                    + job.getStatus().getError());
        }
    }

    private void createSpannerDatabase(String tableName) throws InterruptedException, ExecutionException {
        assert(this.db != null);
        OperationFuture<Database, CreateDatabaseMetadata> op =
            this.dbAdminClient.createDatabase(
                this.db.getInstanceId().getInstance(),
                this.db.getDatabase(),
                Arrays.asList(
                    "CREATE TABLE " + tableName + " ("
                        + "  StartTime   FLOAT64 NOT NULL,"
                        + "  Latency  FLOAT64 NOT NULL,"
                        + ") PRIMARY KEY (StartTime)"));
        try {
            // Initiate the request which returns an OperationFuture.
            Database db = op.get();
            System.out.println("Created database [" + db.getId() + "]");
        } catch (InterruptedException e) {
            System.err.println("Failed to create Spanner database with InterruptedException: " + e.toString());
            throw e;
        } catch (ExecutionException e) {
            System.err.println("Failed to create Spanner database with ExecutionException: " + e.toString());
            throw e;
        }
    }

    public void insertSpannerRow(String tableName, long startTime, long latency) {
        // Insert 4 singer records
        this.dbClient
            .readWriteTransaction()
            .run(transaction -> {
              String sql =
                  "INSERT INTO " + tableName + " (StartTime, Latency) VALUES "
                      + "(" + Long.toString(startTime) + ", " + Long.toString(latency) + ")";
              long rowCount = transaction.executeUpdate(Statement.of(sql));
              System.out.printf("%d records inserted.\n", rowCount);
              return null;
            });
      }
}
