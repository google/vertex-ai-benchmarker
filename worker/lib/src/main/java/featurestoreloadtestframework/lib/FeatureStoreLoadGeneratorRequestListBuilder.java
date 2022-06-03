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
import java.util.ArrayList;
import java.util.List;

public class FeatureStoreLoadGeneratorRequestListBuilder implements LoadGeneratorRequestListBuilder<
    FeatureStoreLoadGenerator> {

    private FeatureStoreApiCaller caller;
    private List<FeatureStoreInput> input;
    private FeaturestoreInputFilesParser fileParser;

    private FeatureStoreLoadGeneratorRequestListBuilder(){}

    public static FeatureStoreLoadGeneratorRequestListBuilder builderForInputList(
        FeatureStoreApiCallerBuilder builder, List<FeatureStoreInput> input) {
        FeatureStoreLoadGeneratorRequestListBuilder fb = new FeatureStoreLoadGeneratorRequestListBuilder();
        fb.input = input;
        fb.caller = builder.createFeatureStoreApiCaller();
        return fb;
    }

    public static FeatureStoreLoadGeneratorRequestListBuilder builderForBigQueryInput(
        FeatureStoreApiCallerBuilder builder, String query, String requestTemplateFile) {
        FeatureStoreLoadGeneratorRequestListBuilder fb = new FeatureStoreLoadGeneratorRequestListBuilder();
        fb.fileParser = new FeaturestoreInputFilesParser(requestTemplateFile, query, true);
        fb.caller = builder.createFeatureStoreApiCaller();
        return fb;
    }

    public static FeatureStoreLoadGeneratorRequestListBuilder builderForTemplateAndEntityFiles(
        FeatureStoreApiCallerBuilder builder, String entityFilePath, String requestTemplateFile) {
        FeatureStoreLoadGeneratorRequestListBuilder fb = new FeatureStoreLoadGeneratorRequestListBuilder();
        fb.fileParser = new FeaturestoreInputFilesParser(requestTemplateFile, entityFilePath);
        fb.caller = builder.createFeatureStoreApiCaller();
        return fb;
    }

    public static FeatureStoreLoadGeneratorRequestListBuilder builderForTemplateAndEntityFiles(
        FeatureStoreApiCallerBuilder builder, String requestFilePath) {
        FeatureStoreLoadGeneratorRequestListBuilder fb = new FeatureStoreLoadGeneratorRequestListBuilder();
        fb.fileParser = new FeaturestoreInputFilesParser(requestFilePath);
        fb.caller = builder.createFeatureStoreApiCaller();
        return fb;
    }

    public List<FeatureStoreLoadGenerator> generateRequestList() {
        List<FeatureStoreInput> inputs = generateInput();
        List<FeatureStoreLoadGenerator> ret = new ArrayList<FeatureStoreLoadGenerator>();
        for (FeatureStoreInput input : inputs) {
            ret.add(new FeatureStoreLoadGenerator(input, caller));
        }
        return ret;
    }

    private List<FeatureStoreInput> generateInput() {
        if (input == null) {
            try {
                input = fileParser.contentsToFeaturestoreInput();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to parse input");
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to parse input");
            }
        }
        return input;
    }

}