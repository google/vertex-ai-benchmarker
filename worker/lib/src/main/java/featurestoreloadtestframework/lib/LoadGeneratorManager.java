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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.time.Instant;
import java.lang.StringBuffer;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ObjectUtils.Null;


public class LoadGeneratorManager<T> {
	public enum SAMPLE_STRATEGY {
		IN_ORDER,
		SHUFFLED,
	}

	private LoadGenerator<T>[] workQueue;
	private List<LoadGenerator<T>> requestList;
	private int targetQPS;
	private int numberThreads;
	private int numberWarmupSamples;
	private int numberSamples;
	private SAMPLE_STRATEGY sampleStrategy;
	private Sleeper sleeper;
	private List<Duration> requestStats;
	private List<FeatureStoreLoadTestResult> fullResult;
	private Long seed;
	private String blobLocation = "";
	private String blobBucket = "";
	private String aggregatedResultsPath = "";
	private ChartWriter chartWriter = null;
	private String bqDatasetName = "";
	private String formattedDate = "";
	private String uuid = "";

	final String GsOriginalPathFormat = "^gs://(?<bucket>[^/]+)/?(?<blob>.*)$";
	final String GsFinalPathFormat = "^gs://(?<bucket>[^/]+)/(?<blob>.+)$";
	final int MaxStringLength = 2000000000;

	private LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
		                     int numberWarmupSamples, int numberSamples, String blobLocation,
                             Sleeper sleeper, String projectId, String location, String datasetId) {
		this.targetQPS = targetQPS;
		this.numberThreads = numberThreads;
		this.sampleStrategy = sampleStrategy;
		this.numberWarmupSamples = numberWarmupSamples;
		this.numberSamples = numberSamples;
		this.sleeper = sleeper;
		this.requestStats = new ArrayList();
		this.bqDatasetName = datasetId;
		// TODO: Potentially rename class name FeatureStoreLoadTestResult to something more generic.
		this.fullResult = new ArrayList<FeatureStoreLoadTestResult>();
		if (StringUtils.isNotBlank(blobLocation)) {
			this.chartWriter = new ChartWriter(projectId, location);
			this.getGCSFilePaths(blobLocation);
		}
	}

	private void getGCSFilePaths(String blobLocation) {
		// Validate format and split bucket from path
		if (!Pattern.matches(GsOriginalPathFormat, blobLocation)){
			throw new IllegalArgumentException(String.format(
				"Invalid GCS path: `%s`", blobLocation));
		}
		if (!blobLocation.endsWith("/")) {
			blobLocation += "/";
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss", Locale.US);
		this.formattedDate = sdf.format(new Date());
		this.uuid = UUID.randomUUID().toString();
		String aggregatedResult = blobLocation + String.format("aggregated_results_%s_%s.txt",
						this.formattedDate, this.uuid);
		Matcher aggregatedResultMatcher = Pattern.compile(GsFinalPathFormat).matcher(aggregatedResult);
		aggregatedResultMatcher.find();
		this.blobBucket = aggregatedResultMatcher.group("bucket");
		this.aggregatedResultsPath = aggregatedResultMatcher.group("blob");
		if (this.bqDatasetName == "") {
			this.bqDatasetName = String.format("vertex_ai_benchmarker_results_%d_qps_%s", this.targetQPS, this.uuid);
		}
		this.blobLocation = blobLocation;
	}

	public LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
								int numberWarmupSamples, int numberSamples, String blobLocation,
								LoadGeneratorRequestListBuilder builder) {
		this(targetQPS, numberThreads, sampleStrategy, numberWarmupSamples, numberSamples,
             blobLocation, new ThreadSleeper(), "", "", "");
		this.requestList = builder.generateRequestList();
	}

	public LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
								int numberWarmupSamples, int numberSamples, String blobLocation,
								LoadGeneratorRequestListBuilder builder, String projectId, String location) {
		this(targetQPS, numberThreads, sampleStrategy, numberWarmupSamples, numberSamples,
             blobLocation, new ThreadSleeper(), projectId, location, "");
		this.requestList = builder.generateRequestList();
	}

	public LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
								int numberWarmupSamples, int numberSamples, String blobLocation,
								LoadGeneratorRequestListBuilder builder, String projectId, String location, String datasetId) {
		
		this(targetQPS, numberThreads, sampleStrategy, numberWarmupSamples, numberSamples,
             blobLocation, new ThreadSleeper(), projectId, location, "");
        this.seed = seed;
        this.requestList = builder.generateRequestList();
    }

	private LoadGenerator<T>[] generateWorkQueue(SAMPLE_STRATEGY sampleStrategy) {
		switch (sampleStrategy) {
			case IN_ORDER:
				return requestList.toArray((LoadGenerator<T>[]) new LoadGenerator[0]);
			case SHUFFLED:
				LoadGenerator<T>[] t = (LoadGenerator<T>[]) new LoadGenerator[requestList.size()];
				int index = 0;
				List<LoadGenerator> temp = new ArrayList<LoadGenerator>();
				temp.addAll(requestList);
				Random r = seed == null ? new Random() : new Random(seed);
				while (!temp.isEmpty()) {
					t[index] = temp.remove(r.nextInt(temp.size()));
					index++;
				}
				return t;
			default:
				throw new RuntimeException(
						"Please define an implementation for the sample strategy '" + sampleStrategy + "'.");
		}
	}

	/**
	 * Write a string to output. This can either be standard out or a GCS blob location if defined.
	 * @param content The string to write.
	 */
	private void writeStrToOutput(String filePath, String content) {
		if (this.blobBucket.isEmpty()) {
			System.out.println(content);
			return;
		}

		if (this.chartWriter == null) {
			System.out.println(content);
			return;
		}
		this.chartWriter.write(this.blobBucket, filePath, content);
	}

	private String getNewBlobAndTableNames(int detailedResultIndex) {
		String detailResult = this.blobLocation + String.format("detailed_results_%s_%s_%d.csv",
					this.formattedDate, this.uuid, detailedResultIndex);
		Matcher detailResultMatcher = Pattern.compile(GsFinalPathFormat).matcher(detailResult);
		detailResultMatcher.find();
		String detailResultPath = detailResultMatcher.group("blob");
		return detailResultPath;
	}

	private void WriteContent(WritableByteChannel channel, StringBuilder content, String bqTableName, String detailResultPath, WriteDisposition option) throws InterruptedException, IOException {
		try {
			channel.write(ByteBuffer.wrap(content.toString().getBytes(StandardCharsets.UTF_8)));
		} catch (IOException e) {
			System.err.println("Failed to write new content to detailed results csv file: " + e.getMessage());
			throw e;
		}

		channel.close();

		this.chartWriter.exportToBQ(this.bqDatasetName, bqTableName, String.format("gs://%s/%s", this.blobBucket, detailResultPath), option);
	}

	private void writeFullResultToCSV() throws IOException, BigQueryException, InterruptedException {
		String csvHeader = "StartTime,Duration\n";
		if (this.chartWriter == null) {
			return;
		}
		WriteDisposition option = WriteDisposition.WRITE_TRUNCATE;
		String bqTableName = String.format("loadtest_result_table_%s_%s", formattedDate, uuid);

		int detailedResultIndex = 1;

		String detailResultPath = this.getNewBlobAndTableNames(detailedResultIndex);
		Blob detailedBlob = this.chartWriter.write(this.blobBucket, detailResultPath, "");

		StringBuilder content = new StringBuilder(csvHeader);
		WritableByteChannel channel = detailedBlob.writer();

		for (FeatureStoreLoadTestResult result : this.fullResult) {
			content.append(result.toString() + "\n");

			// Write to csv file and append to BQ table before string exceeds Java max string length.
			if (content.length() > this.MaxStringLength) {
				this.WriteContent(channel, content, bqTableName, detailResultPath, option);
				detailedResultIndex ++;

				detailResultPath = this.getNewBlobAndTableNames(detailedResultIndex);
				detailedBlob = this.chartWriter.write(this.blobBucket, detailResultPath, "");

				content = new StringBuilder(csvHeader);
				channel = detailedBlob.writer();
				option = WriteDisposition.WRITE_APPEND;
			}
		}

		this.WriteContent(channel, content, bqTableName, detailResultPath, option);
	}

	private void runSamples(int numSamples, boolean keepStats) throws InterruptedException {
		ExecutorService pool = Executors.newCachedThreadPool();

		AtomicInteger numberExceededTime = new AtomicInteger();
		int index = 0;
		for (int sampleNum = 0; sampleNum < numSamples; sampleNum++) {
			long start = System.currentTimeMillis();
			long stop = start  + 1000;
			int paramSampleNum;

			// Freeze values for Runnable.
			final int _index = index;
			final int _sampleNum = sampleNum;
			final long _stop = stop;
			Future<?> future = pool.submit(() -> {
				try {
					runSample(_index, keepStats);
					long end = System.currentTimeMillis();
					if (end > _stop) {
						System.out.println(
								"[Sample " + _sampleNum + "] Unable to reach desired QPS.");
						numberExceededTime.incrementAndGet();
					} else {
						System.out.println("[Sample " + _sampleNum + "] Reached target QPS.");
					}
				} catch (Exception e) {
					System.out.println("[Sample " + _sampleNum + "] Exception: " + e);
					e.printStackTrace();
				}
			});

			index += targetQPS;
			index = index % workQueue.length;

			try {
				sleeper.sleep(stop - System.currentTimeMillis());
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		pool.shutdown();
		if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
			System.out.println("Tasks are still pending!");
		}
	}

	private void runExperiment() throws InterruptedException, IOException, BigQueryException {

		System.out.println("Running warmup samples.");
		runSamples(this.numberWarmupSamples, false); // Don't keep stats for warmup samples.
		System.out.println();
		System.out.println("Running samples.");
		runSamples(this.numberSamples, true); // Record stats for samples after warmup is done.

		String statString = CalculateStats();
		writeStrToOutput(this.aggregatedResultsPath, statString);
		writeFullResultToCSV();
	}

	private Duration percentile(List<Duration> stats, double percentile) {
		int index = (int) Math.ceil(percentile / 100.0 * stats.size());
    		return stats.get(index-1);
	}

	private Duration interpolation(List<Duration> sortedStats, double percentile) {
		int floor = (int) Math.floor(percentile * (sortedStats.size() - 1) / 100);
		Duration y0 = sortedStats.get(floor);

		Duration y1 = sortedStats.get(floor + 1);
		double perBucket = 100.0D / (sortedStats.size() - 1);

		// interpolation = (x - x0) * (y1 - y0) + y0
		double difference = (percentile - (perBucket * floor)) / perBucket;
		// (x - x0) * (y1 - y0). Duration methods can only multiply by a long - so convert to nanos,
		// multiply by the double, and convert back.
		Duration yDiff = y1.minus(y0);
		Duration interpol = Duration.ofNanos(
				Math.round(difference * yDiff.toNanos()));
		interpol = interpol.plus(y0);  // + y0
		return interpol;
	}

	private String CalculateStats() {
		if (requestStats.isEmpty()) {
			System.out.println("No stats to calculate yet!");
			return StringUtils.EMPTY;
		}
		double average = 0.0D;
		for (Duration curr : requestStats) {
			long durationInMillis = curr.toMillis();
			average += durationInMillis;
		}
		average = average / requestStats.size();

		List<Duration> copiedStats = new ArrayList(requestStats);
		Collections.sort(copiedStats);

		long min = copiedStats.get(0).toMillis();
		long max = copiedStats.get(copiedStats.size() - 1).toMillis();
		long p90 = interpolation(copiedStats, 90).toMillis();
		long p95 = interpolation(copiedStats, 95).toMillis();
		long p99 = interpolation(copiedStats, 99).toMillis();

		return String.format(
				"Min: %dms, Max: %dms, Average: %.2fms, P90: %dms, P95: %dms, P99: %dms\n",
				min, max, average, p90, p95, p99);
	}

	private void verifyBucketExists() throws Exception {
		Storage storage = StorageOptions.getDefaultInstance().getService();
		Bucket bucket = storage.get(this.blobBucket);
		if (bucket == null) {
			throw new Exception(String.format("Unable to find bucket `%s`", this.blobBucket));
		}
	}

	/**
	 * Will use items from the workQueue from index to index+targetQps % workQueue.length.
	 * @param startIndex Which index in the work queue should sample start at?
	 * @param keepStats Should the statistics be calculated? This is useful to turn off when running
	 *                  initial samples which may be slow due to gRPC channel creation, other cache
	 *                  warming, etc.
	 */
	private void runSample(int startIndex, boolean keepStats) {
		MetricsThreadPoolExecutor executor = new MetricsThreadPoolExecutor(
			this.numberThreads);
		for (int x = 0; x < targetQPS; x++) {
			executor.submit(workQueue[(startIndex + x) % workQueue.length]);
		}
		try {
			executor.shutdown();
			if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
				requestStats.addAll(executor.getRequestStats());
				this.fullResult.addAll(executor.getFullResult());
				throw new RuntimeException(String.format(
					"Pending requests failed to execute."));
			}
		} catch(InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
			e.printStackTrace();
		}

		if (keepStats) {
			requestStats.addAll(executor.getRequestStats());
			this.fullResult.addAll(executor.getFullResult());
		}
	}

	protected LoadGenerator<T>[] getWorkQueue() {
		return workQueue;
	}

	public void run() throws WorkTimeoutException, Exception, BigQueryException {
		if (!this.blobBucket.isEmpty()) {
			verifyBucketExists();
		}
		workQueue = generateWorkQueue(sampleStrategy);
		try {
			runExperiment();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
