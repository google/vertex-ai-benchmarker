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
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import org.apache.commons.lang3.time.DurationFormatUtils;

public class FeatureStoreLoadTestResult {
    private Instant startTime;
    private Duration latency;

    public FeatureStoreLoadTestResult(Instant startTime, Duration latency) {
        this.startTime = startTime;
        this.latency = latency;
    }

    public Instant getStartTime() {
        return this.startTime;
    }

    public Duration getLatency() {
        return this.latency;
    }

    public String toString() {
        String dateTimeFormatter = "yyyy-MM-dd HH:mm:ss.SSSSSS";
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateTimeFormatter).withZone(ZoneId.systemDefault());
        String timeFormatter = "HH:mm:ss.SSSSSS";
        String startTime = dateFormatter.format(this.startTime);
        String latency = DurationFormatUtils.formatDuration(this.latency.toMillis(), timeFormatter);

        return startTime + "," + latency;
    }
}
