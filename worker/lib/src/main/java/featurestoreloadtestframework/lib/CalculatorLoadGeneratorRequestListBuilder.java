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
import java.util.List;
import java.util.Random;

public class CalculatorLoadGeneratorRequestListBuilder implements LoadGeneratorRequestListBuilder<CalculatorLoadGenerator> {
	private int numEntries;
    private List<CalculatorLoadGenerator> inputList;

	public CalculatorLoadGeneratorRequestListBuilder(int numEntries) {
		this.numEntries = numEntries;
	}

    public CalculatorLoadGeneratorRequestListBuilder(List<CalculatorLoadGenerator> inputList) {
        this.inputList = inputList;
    }

	public List<CalculatorLoadGenerator> generateRequestList() {
        if (inputList == null) {
    		Random r = new Random();
    		List<CalculatorLoadGenerator> ret = new ArrayList();
    		for (int i = 0; i < numEntries; i++) {
    			IntegerPair ip = new IntegerPair(r.nextInt(1000), r.nextInt(1000));
    			ret.add(new CalculatorLoadGenerator(ip));
    		}
    		return ret;
        }
        return inputList;
	}
}

