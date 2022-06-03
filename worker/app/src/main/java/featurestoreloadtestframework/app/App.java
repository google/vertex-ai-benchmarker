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

package featurestoreloadtestframework.app;

import featurestoreloadtestframework.lib.FeatureStoreApiCaller.REST_METHOD;
import featurestoreloadtestframework.lib.FeatureStoreApiCallerBuilder;
import featurestoreloadtestframework.lib.FeatureStoreApiCallerBuilder.API_VERSION;
import featurestoreloadtestframework.lib.FeatureStoreInput;
import featurestoreloadtestframework.lib.FeatureStoreLoadGenerator;
import featurestoreloadtestframework.lib.FeaturestoreInputFilesParser;
import featurestoreloadtestframework.lib.FeatureStoreLoadGeneratorRequestListBuilder;
import featurestoreloadtestframework.lib.LoadGeneratorManager;

import featurestoreloadtestframework.lib.LoadGeneratorManager.SAMPLE_STRATEGY;
import featurestoreloadtestframework.lib.LoadGeneratorRequestListBuilder;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;

import java.util.List;

public class App {

  /**
   * Command line options for the load test tool.
   */
  static class AppOptions {

    @Option(name = "--target_qps", usage = "Target queries per second")
    int targetQps = 1;

    @Option(name = "--num_threads", usage = "Number of threads to use for sending requests.")
    int numThreads = 1;

    @Option(name = "--num_samples", usage = "Number of samples. Each samples will try to generate the " +
        "target QPS per second. In effect, this is also the number of seconds the test will run for.")
    int numSamples = 10;

    @Option(name = "--num_warmup_samples", usage = "Number of warmup samples.")
    int numWarmupSamples = 5;

    @Option(name = "--sample_strategy", usage = "Sample strategy.")
    LoadGeneratorManager.SAMPLE_STRATEGY sampleStrategy = SAMPLE_STRATEGY.SHUFFLED;

    @Option(name = "--project_id", usage = "Project ID.", required = true)
    String projectId = null;

    @Option(name = "--region", usage = "The cloud region the feature store(s) are located in.", required = true)
    String region = null;

    @Option(name = "--gcs_output_path", usage = "GCS output path.")
    String gcsOutputPath = null;

    @Option(name = "--feature_query_file", usage = "File specifying features to query for.",
        required = true)
    String featureQueryFile = null;

    @Option(name = "--entity_file", usage = "File specifying which entities to fetch feature values for.",
        required = true)
    String entityFile = null;

    @Option(name = "--bigquery_output_dataset", usage = "BigQuery output dataset location.")
    String bigqueryOutputDataset = "";

    // Future options.
    final FeatureStoreApiCallerBuilder.API_VERSION apiVersion = API_VERSION.V1beta1;
  }

  public static void main(String[] args) {
    AppOptions options = new AppOptions();
    CmdLineParser parser = new CmdLineParser(options);

    for (String arg : args) {
      if (arg.equals("-h") || arg.equals("--help")) {
        System.err.println("USAGE:");
        parser.printUsage(System.err);
        System.exit(0);
      }
    }

    try {
      if (args.length == 0) {
        throw new CmdLineException(parser, Messages.NO_ARGUMENT_GIVEN);
      }

      parser.parseArgument(args);
    } catch (CmdLineException e) {
      String msg = e.getMessage();
      if (StringUtils.isNotBlank(msg)) {
        System.err.println("ERROR: " + msg);
        System.err.println();
      }
      System.err.println("USAGE:");
      parser.printUsage(System.err);
      System.exit(1);
    }

    /* Setup feature store inputs. */
    // TODO: Change inputs if BQ is used instead of an entity file. Also need to define a BQ
    //       flag and make entityFile XOR repeated BQ flag be true.
    // FeaturestoreInputFilesParser fp = new FeaturestoreInputFilesParser(options.featureQueryFile, options.bqQuery, true);
    FeaturestoreInputFilesParser fp = new FeaturestoreInputFilesParser(options.featureQueryFile,
        options.entityFile);
    List<FeatureStoreInput> inputs = null;
    try {
      inputs = fp.contentsToFeaturestoreInput();
    } catch (InterruptedException | IOException e) {
      System.err.println("Failed to parse feature store inputs: " + e + ".");
      e.printStackTrace();
      System.exit(1);
    }

    if (inputs.isEmpty()) {
      System.err.println("No feature value queries generated - please check your input files.");
      System.exit(1);
    }

    FeatureStoreApiCallerBuilder apiCallBuilder = new FeatureStoreApiCallerBuilder(
        /* apiVersion = */ options.apiVersion,
        /* project = */ options.projectId,
        /* location = */ options.region,
        /* method = */ REST_METHOD.GET);

    LoadGeneratorRequestListBuilder<FeatureStoreLoadGenerator> builder = FeatureStoreLoadGeneratorRequestListBuilder.builderForInputList(
        /* builder = */ apiCallBuilder,
        /* providedInputs = */ inputs);

    LoadGeneratorManager<FeatureStoreInput> lg = new LoadGeneratorManager<>(
        /* targetQPS = */ options.targetQps,
        /* numberThreads = */ options.numThreads,
        /* sampleStrategy = */ options.sampleStrategy,
        /* numberWarmupSamples = */ options.numWarmupSamples,
        /* numberSamples = */ options.numSamples,
        /* blobLocation = */ options.gcsOutputPath,
        /* builder = */ builder,
        /* projectId = */ options.projectId,
        /* location = */ options.region,
        /* datasetId = */ options.bigqueryOutputDataset);

    try {
      lg.run();
    } catch (Exception e) {
      System.out.print("Exception running load generator manager:");
      e.printStackTrace(System.out);
      System.exit(1);
    }
    System.out.println("End!");
  }
}
