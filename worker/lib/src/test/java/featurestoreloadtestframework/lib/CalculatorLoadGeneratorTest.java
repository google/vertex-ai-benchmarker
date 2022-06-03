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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CalculatorLoadGeneratorTest {

    @Test void testInOrder() throws Exception {
        List<CalculatorLoadGenerator> requestList = Arrays.asList(
                new CalculatorLoadGenerator(new IntegerPair(1, 2)),
                new CalculatorLoadGenerator(new IntegerPair(2, 2)),
                new CalculatorLoadGenerator(new IntegerPair(3, 2)),
                new CalculatorLoadGenerator(new IntegerPair(4, 2)),
                new CalculatorLoadGenerator(new IntegerPair(5, 2)),
                new CalculatorLoadGenerator(new IntegerPair(6, 2))
            );
        LoadGeneratorManager testLoadGenerator = new LoadGeneratorManager(
            1,
            1,
            LoadGeneratorManager.SAMPLE_STRATEGY.IN_ORDER,
            0,
            6,
            "",
            new CalculatorLoadGeneratorRequestListBuilder(requestList),
            0L,
            new TestSleeper());
        try {
            testLoadGenerator.run();
        } catch (Exception e) {
            fail(e);
        }
        List<Integer> aggregatedResults = new ArrayList<Integer>();
        for (CalculatorLoadGenerator clg : requestList) {
            aggregatedResults.addAll(clg.getCalculations());
        }
        assertEquals(Arrays.asList(3, 4, 5, 6, 7, 8), aggregatedResults);
    }

    @Test void testShuffled() throws Exception {
        List<CalculatorLoadGenerator> requestList = Arrays.asList(
                new CalculatorLoadGenerator(new IntegerPair(1, 2)),
                new CalculatorLoadGenerator(new IntegerPair(2, 2)),
                new CalculatorLoadGenerator(new IntegerPair(3, 2)),
                new CalculatorLoadGenerator(new IntegerPair(4, 2)),
                new CalculatorLoadGenerator(new IntegerPair(5, 2)),
                new CalculatorLoadGenerator(new IntegerPair(6, 2))
            );
        LoadGeneratorManager testLoadGenerator = new LoadGeneratorManager(
            1,
            1,
            LoadGeneratorManager.SAMPLE_STRATEGY.SHUFFLED,
            0,
            6,
            "",
            new CalculatorLoadGeneratorRequestListBuilder(requestList),
            0L,
            new TestSleeper());
        try {
            testLoadGenerator.run();
        } catch (Exception e) {
            fail(e);
        }
        List<Integer> aggregatedResults = new ArrayList<Integer>();
        for (LoadGenerator clg : testLoadGenerator.getWorkQueue()) {
            aggregatedResults.addAll(((CalculatorLoadGenerator) clg).getCalculations());
        }
        assertEquals(Arrays.asList(3, 7, 4, 8, 6, 5), aggregatedResults);
    }

}
