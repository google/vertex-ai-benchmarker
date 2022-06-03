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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

class MetricsThreadPoolExecutor extends ThreadPoolExecutor {

    private Map<String, Instant> startTimes;
	private Vector<Duration> requestStats;
    private Vector<FeatureStoreLoadTestResult> fullResult;

	public MetricsThreadPoolExecutor(int poolSize) {
        super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
        this.requestStats = new Vector();
        this.fullResult = new Vector<FeatureStoreLoadTestResult>();
        this.startTimes = new HashMap<String, Instant>();
	}

    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        String name = r.toString();
        name = name.substring(name.indexOf("@"), name.indexOf("["));
        startTimes.put(name, Instant.now());
    }

    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        String name = r.toString();
        name = name.substring(name.indexOf("@"), name.indexOf("["));
        Instant startTime = startTimes.get(name);
        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);
        requestStats.add(duration);
        this.fullResult.add(new FeatureStoreLoadTestResult(startTime, duration));
    }

    // RequestStats contains a list of duration for each call. 
    public List<Duration> getRequestStats() {
        return requestStats;
    }

    // FullResult contains a list of StartTime : Duration pair for each call. 
    public Vector<FeatureStoreLoadTestResult> getFullResult() {
        return this.fullResult;
    }
}