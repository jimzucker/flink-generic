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

package com.ness.flink.example.pipeline.domain.intermediate;

import com.ness.flink.example.pipeline.domain.InterestRate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class InterestRatesTest {

    private static InterestRate rate(String maturity, long timestamp) {
        InterestRate rate = InterestRate.builder().interestRateId(1).maturity(maturity).rate(2.5).build();
        rate.setTimestamp(timestamp);
        return rate;
    }

    @Test
    void shouldBeEmptyInitially() {
        Assertions.assertTrue(new InterestRates().empty());
    }

    @Test
    void shouldStoreRatesByMaturityAndTrackLatestTimestamp() {
        InterestRates rates = new InterestRates();
        rates.add(rate("1Y", 1_000L));
        rates.add(rate("2Y", 3_000L));

        Assertions.assertFalse(rates.empty());
        Assertions.assertEquals(2, rates.getRates().size());
        Assertions.assertTrue(rates.getRates().containsKey("1Y"));
        // storeTimestamp keeps the maximum element timestamp
        Assertions.assertEquals(3_000L, rates.getTimestamp());
    }

    @Test
    void shouldReplaceRateForSameMaturity() {
        InterestRates rates = new InterestRates();
        rates.add(rate("1Y", 1_000L));
        rates.add(rate("1Y", 2_000L));

        Assertions.assertEquals(1, rates.getRates().size());
        Assertions.assertEquals(2_000L, rates.getRates().get("1Y").getTimestamp());
    }

    @Test
    void kafkaKeyShouldBeCurrencyBytes() {
        Assertions.assertArrayEquals("USD".getBytes(StandardCharsets.UTF_8), new InterestRates().kafkaKey());
    }
}
