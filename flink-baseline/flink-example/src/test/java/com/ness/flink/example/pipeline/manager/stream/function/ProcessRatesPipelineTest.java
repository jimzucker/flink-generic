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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * In-process MiniCluster test of the assembled interest-rate operator chain
 * (bounded source -&gt; event-time watermarks -&gt; keyBy -&gt; {@link ProcessRatesFunction}).
 *
 * <p>Closes the coverage gap between the isolated operator-harness test and the full docker-stack
 * SmoothingIT: it verifies that a real source's advancing watermarks fire the window timer and the
 * operator actually EMITS {@code InterestRates}. A regression where the windowed operator produces
 * nothing (the SmoothingIT failure mode) would fail here — deterministically and in seconds, with
 * no external Kafka/Redis/MySQL.
 */
class ProcessRatesPipelineTest {

    private static InterestRate rate(int id, String maturity, double value, long timestamp) {
        InterestRate rate = InterestRate.builder().interestRateId(id).maturity(maturity).rate(value).build();
        rate.setTimestamp(timestamp);
        return rate;
    }

    @Test
    void shouldEmitAggregatedInterestRatesThroughWindowedOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setParallelism(1);
        // ProcessRatesFunction#open reads watermark/window config from global job params + application.yml.
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(Map.of()));

        // All timestamps fall in the same 10s window; the bounded source flushes a final (MAX)
        // watermark at end-of-input which must fire the event-time timer and emit the snapshot.
        List<InterestRate> input = List.of(
            rate(1, "1Y", 2.5, 1_000L),
            rate(2, "2Y", 3.0, 2_000L),
            rate(1, "1Y", 2.9, 5_000L)); // newer value for 1Y supersedes the earlier one

        SingleOutputStreamOperator<InterestRates> emitted = env
            .fromData(input, TypeInformation.of(InterestRate.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<InterestRate>forMonotonousTimestamps()
                    .withTimestampAssigner((event, ts) -> event.getTimestamp()))
            .keyBy(r -> InterestRates.EMPTY_RATES.getCurrency())
            .process(new ProcessRatesFunction());

        List<InterestRates> results = new ArrayList<>();
        try (CloseableIterator<InterestRates> it = emitted.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }

        Assertions.assertFalse(results.isEmpty(),
            "Windowed operator emitted no InterestRates — the SmoothingIT failure mode");
        InterestRates last = results.get(results.size() - 1);
        Assertions.assertEquals(2, last.getRates().size());
        Assertions.assertTrue(last.getRates().containsKey("1Y"));
        Assertions.assertTrue(last.getRates().containsKey("2Y"));
        Assertions.assertEquals(2.9, last.getRates().get("1Y").getRate());
    }
}
