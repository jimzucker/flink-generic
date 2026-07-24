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

package com.ness.flink.example.pipeline.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.ness.flink.example.pipeline.config.JobMode;
import com.ness.flink.example.pipeline.manager.stream.InterestRateStream;
import com.ness.flink.example.pipeline.manager.stream.OptionPriceStream;
import com.ness.flink.stream.StreamBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Covers the {@link FlinkJobManager#buildStreams} mode dispatch without running a Flink job, by
 * stubbing the static stream builders and verifying which ones each {@link JobMode} wires up.
 */
class FlinkJobManagerTest {

    @Test
    void fullModeBuildsInterestRatesAndOptionPrices() {
        StreamBuilder streamBuilder = mock(StreamBuilder.class);
        try (MockedStatic<InterestRateStream> interestRates = mockStatic(InterestRateStream.class);
             MockedStatic<OptionPriceStream> optionPrices = mockStatic(OptionPriceStream.class)) {

            FlinkJobManager.buildStreams(streamBuilder, JobMode.FULL, true);

            interestRates.verify(() -> InterestRateStream.build(streamBuilder, true));
            optionPrices.verify(() -> OptionPriceStream.build(streamBuilder));
        }
    }

    @Test
    void optionPricesOnlyModeBuildsOnlyOptionPrices() {
        StreamBuilder streamBuilder = mock(StreamBuilder.class);
        try (MockedStatic<InterestRateStream> interestRates = mockStatic(InterestRateStream.class);
             MockedStatic<OptionPriceStream> optionPrices = mockStatic(OptionPriceStream.class)) {

            FlinkJobManager.buildStreams(streamBuilder, JobMode.OPTION_PRICES_ONLY, true);

            optionPrices.verify(() -> OptionPriceStream.build(streamBuilder));
            interestRates.verifyNoInteractions();
        }
    }

    @Test
    void interestRatesOnlyModeBuildsOnlyInterestRates() {
        StreamBuilder streamBuilder = mock(StreamBuilder.class);
        try (MockedStatic<InterestRateStream> interestRates = mockStatic(InterestRateStream.class);
             MockedStatic<OptionPriceStream> optionPrices = mockStatic(OptionPriceStream.class)) {

            FlinkJobManager.buildStreams(streamBuilder, JobMode.INTEREST_RATES_ONLY, false);

            interestRates.verify(() -> InterestRateStream.build(streamBuilder, false));
            optionPrices.verifyNoInteractions();
        }
    }
}
