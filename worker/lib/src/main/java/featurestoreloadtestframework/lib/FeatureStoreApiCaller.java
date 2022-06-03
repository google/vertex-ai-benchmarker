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

import java.util.List;

public abstract class FeatureStoreApiCaller {
	public enum REST_METHOD {
		GET,
		LIST
	}
	protected String project;
	protected String location;
	protected String endpoint;
	protected REST_METHOD method;  // Evaluate if this should live somewhere else

	public FeatureStoreApiCaller(String project, String location, REST_METHOD method) {
		this.project = project;
		this.location = location;
		this.endpoint = String.format("%s-aiplatform.googleapis.com:443", location);
		this.method = method;
	}

	public FeatureStoreApiCaller(String project, String location, String endpointOverride,
		REST_METHOD method) {
		this.project = project;
		this.location = location;
		this.endpoint = endpointOverride;
		this.method = method;
	}

	public abstract void call(FeatureStoreInput featureStoreInput);

}