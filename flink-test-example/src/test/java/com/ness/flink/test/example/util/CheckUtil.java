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

package com.ness.flink.test.example.util;

import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.test.example.sender.ExpectedResultHolder;
import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.example.pipeline.domain.InterestRate;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.Comparator;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Slf4j
public class CheckUtil {

    public static void checkAggregatedPrices(Map<String, Map<String, OptionPrice>> expectedPrices,
                                             Map<String, SmoothingRequest> actualResults) {
        // Collect every missing underlying (instead of failing on the first) so the failure
        // message shows whether this is a broad completeness/throughput problem or a single key.
        java.util.List<String> missing = new java.util.ArrayList<>();
        expectedPrices.forEach((underlying, expectedOptionPrices) -> {
            SmoothingRequest actualRequest = actualResults.get(underlying);
            if (actualRequest == null) {
                missing.add(underlying);
            } else {
                checkPrices(actualRequest.getOptionPrices(), expectedOptionPrices);
            }
        });
        if (!missing.isEmpty()) {
            missing.sort(Comparator.naturalOrder());
            java.util.List<String> sample = missing.subList(0, Math.min(20, missing.size()));
            Assert.fail(String.format(
                "Missing option prices for %d/%d expected underliers (received %d). Missing sample: %s",
                missing.size(), expectedPrices.size(), actualResults.size(), sample));
        }
    }

    public static void checkPrices(Map<String, OptionPrice> actual, Map<String, OptionPrice> expected) {
        expected.forEach((instrumentId, price) ->
                assertEquals(actual.get(instrumentId), price, "Got wrong price value for underlier " + price.getUnderlying().getName())
        );
    }

    public static void checkInterestRates(Map<String, InterestRate> actualInterestRates, Map<String, InterestRate> expectedInterestRates, String windowId) {
        expectedInterestRates.forEach((expectedMaturity, expectedRate) -> {
            final InterestRate actualInterestRate = actualInterestRates.get(expectedMaturity);
            Assert.assertNotNull(actualInterestRate,
                    "Cannot find expected interestRate by expectedMaturity: " + expectedMaturity + " windowId: " + windowId);
            Assert.assertEquals(actualInterestRate, expectedRate, "Failed to compare InterestRates windowId: " + windowId);
        });
    }

    public static void checkResults(Map<String, ExpectedResultHolder> expectedResults,
                                    Map<String, Map<String, SmoothingRequest>> actualResults,
                                    int numberOfWindows, int numberOfInterestRates) {
        assertEquals(expectedResults.keySet().size(), numberOfWindows);

        expectedResults.forEach((windowId, expectedResultHolder) -> {
            log.info("Checking: windowId={}", windowId);
            // Key: windowID-underlying
            Map<String, SmoothingRequest> underliersOfWindow = actualResults.get(windowId);
            assertNotNull(underliersOfWindow);
            underliersOfWindow.values().stream()
                    .max(Comparator.comparingLong(IncomingEvent::getTimestamp)).ifPresent(p -> {
                long consumeTime = p.getTimestamp();
                long sendTime = expectedResultHolder.getSendTime();
                log.info("Latency for windowId: {}, {} ms", windowId, consumeTime - sendTime);
            });

            Map<String, OptionPrice> allPrices = underliersOfWindow.values().stream()
                    .map(SmoothingRequest::getOptionPrices)
                    .reduce((map1, map2) -> {
                        map1.putAll(map2);
                        return map1;
                    })
                    .orElseThrow();

            checkPrices(allPrices, expectedResultHolder.getData());

            if (numberOfInterestRates > 0) {
                final Map<String, InterestRate> expectedRates = expectedResultHolder.getRates();
                underliersOfWindow.values().forEach(actual -> {
                    checkInterestRates(actual.getInterestRates(), expectedRates, windowId);
                });
            }
        });
    }
}
