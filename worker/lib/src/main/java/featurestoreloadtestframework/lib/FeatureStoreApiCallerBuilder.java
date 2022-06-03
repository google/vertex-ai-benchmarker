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

public class FeatureStoreApiCallerBuilder {
	private API_VERSION apiVersion;
	private String project;
	private String location;
	private FeatureStoreApiCaller.REST_METHOD method;

	public enum API_VERSION {
			V1,
			V1beta1
	}
	
	public FeatureStoreApiCallerBuilder(API_VERSION apiVersion, String project, String location, FeatureStoreApiCaller.REST_METHOD method) {
		this.apiVersion = apiVersion;
		this.project = project;
		this.location = location;
		this.method = method;
	}

	protected FeatureStoreApiCaller createFeatureStoreApiCaller() {
		if (apiVersion == API_VERSION.V1) {
			return new FeatureStoreApiCallerV1(project, location, method);
		}
		if (apiVersion == API_VERSION.V1beta1) {
			return new FeatureStoreApiCallerV1beta1(project, location, method);
		}
		return null;
	}
}