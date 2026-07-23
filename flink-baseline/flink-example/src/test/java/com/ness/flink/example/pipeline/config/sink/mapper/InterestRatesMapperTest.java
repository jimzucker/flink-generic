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

package com.ness.flink.example.pipeline.config.sink.mapper;

import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.snapshot.context.ContextMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InterestRatesMapperTest {

    private final InterestRatesMapper mapper = new InterestRatesMapper(":");

    private static ContextMetadata ctx() {
        return ContextMetadata.builder().contextId(5L).date("20240101").contextName("InterestRates").build();
    }

    @Test
    void shouldComposeSnapshotKeyFromContextAndCurrency() {
        // snapshot{d}contextName{d}date{d}contextId{d}currency
        Assertions.assertEquals("snapshot:InterestRates:20240101:5:USD",
            mapper.buildKey(new InterestRates(), ctx()));
    }

    @Test
    void shouldSerializeValueToJson() {
        String json = mapper.getValueFromData(new InterestRates());

        Assertions.assertTrue(json.contains("USD"), "currency should appear in serialized JSON");
        Assertions.assertTrue(json.contains("rates"), "rates map should appear in serialized JSON");
    }
}
