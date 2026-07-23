/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.example.pipeline.manager.stream.function;

import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Operator-harness test for {@link ProcessRatesFunction}. Establishes the
 * {@link KeyedOneInputStreamOperatorTestHarness} pattern for the example keyed process functions:
 * feed elements, advance event-time watermark to fire the window timer, and assert emitted output.
 */
class ProcessRatesFunctionTest {

    /** All rates share one Flink key so they accumulate into a single InterestRates snapshot. */
    private static final KeySelector<InterestRate, String> CONSTANT_KEY = rate -> "USD";

    private KeyedOneInputStreamOperatorTestHarness<String, InterestRate, InterestRates> harness;

    @BeforeEach
    void setUp() throws Exception {
        KeyedProcessOperator<String, InterestRate, InterestRates> operator =
            new KeyedProcessOperator<>(new ProcessRatesFunction());
        harness = new KeyedOneInputStreamOperatorTestHarness<>(operator, CONSTANT_KEY, Types.STRING);
        // ProcessRatesFunction#open reads watermark config from the global job parameters + application.yml.
        harness.getExecutionConfig().setGlobalJobParameters(ParameterTool.fromMap(Map.of()));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private static InterestRate rate(int id, String maturity, double value, long timestamp) {
        InterestRate rate = InterestRate.builder().interestRateId(id).maturity(maturity).rate(value).build();
        rate.setTimestamp(timestamp);
        return rate;
    }

    @Test
    void shouldEmitAccumulatedRatesWhenWindowTimerFires() throws Exception {
        harness.processElement(rate(1, "1Y", 2.5, 1_000L), 1_000L);
        harness.processElement(rate(2, "2Y", 3.0, 2_000L), 2_000L);

        // No output until the event-time window timer fires.
        Assertions.assertTrue(harness.extractOutputValues().isEmpty());

        // Advance the watermark past the end of the window to fire the registered timer.
        harness.processWatermark(Long.MAX_VALUE);

        List<InterestRates> output = harness.extractOutputValues();
        Assertions.assertEquals(1, output.size());
        InterestRates emitted = output.get(0);
        Assertions.assertEquals(2, emitted.getRates().size());
        Assertions.assertTrue(emitted.getRates().containsKey("1Y"));
        Assertions.assertTrue(emitted.getRates().containsKey("2Y"));
    }

    @Test
    void shouldKeepOnlyLatestRatePerMaturity() throws Exception {
        harness.processElement(rate(1, "1Y", 2.5, 1_000L), 1_000L);
        // Newer timestamp for the same maturity replaces the previous value.
        harness.processElement(rate(1, "1Y", 2.9, 5_000L), 5_000L);

        harness.processWatermark(Long.MAX_VALUE);

        List<InterestRates> output = harness.extractOutputValues();
        Assertions.assertEquals(1, output.size());
        InterestRates emitted = output.get(0);
        Assertions.assertEquals(1, emitted.getRates().size());
        Assertions.assertEquals(2.9, emitted.getRates().get("1Y").getRate());
    }
}
