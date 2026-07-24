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

package com.ness.flink.snapshot.context;

import com.ness.flink.util.DateTimeUtils;
import com.ness.flink.window.generator.impl.BasicGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WindowBasedTest {

    @Test
    void shouldBuildContextMetadataFromWindow() {
        WindowBased service = new WindowBased(new BasicGenerator(10_000));

        // timestamp 1000 falls in window 1 (start epoch 0) for a 10s window rooted at 0.
        ContextMetadata metadata = service.generate(() -> 1_000L, "InterestRates");

        Assertions.assertEquals(1L, metadata.getContextId());
        Assertions.assertEquals("InterestRates", metadata.getContextName());
        Assertions.assertEquals(DateTimeUtils.formatDate(0L), metadata.getDate());
    }

    @Test
    void shouldAdvanceContextIdAcrossWindows() {
        WindowBased service = new WindowBased(new BasicGenerator(10_000));

        long firstWindow = service.generate(() -> 1_000L, "ctx").getContextId();
        long laterWindow = service.generate(() -> 55_000L, "ctx").getContextId();

        Assertions.assertTrue(laterWindow > firstWindow,
            "context id must increase for a later window");
    }
}
