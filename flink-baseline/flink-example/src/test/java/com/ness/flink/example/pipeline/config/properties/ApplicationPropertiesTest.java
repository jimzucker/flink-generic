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

package com.ness.flink.example.pipeline.config.properties;

import com.ness.flink.example.pipeline.config.JobMode;
import com.ness.flink.storage.cache.EntityTypeEnum;
import com.ness.flink.window.generator.WindowGeneratorProvider;
import java.util.Map;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApplicationPropertiesTest {

    @Test
    void shouldUseDefaultsWhenNoConfigProvided() {
        ApplicationProperties props = ApplicationProperties.from(ParameterTool.fromMap(Map.of()));

        Assertions.assertEquals(JobMode.FULL, props.getJobMode());
        Assertions.assertEquals(EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY, props.getSnapshotType());
        Assertions.assertEquals(WindowGeneratorProvider.GeneratorType.BASIC, props.getWindowGeneratorType());
        Assertions.assertFalse(props.isInterestRatesKafkaSnapshotEnabled());
        Assertions.assertFalse(props.isEnabledExtendedLogging());
        Assertions.assertNotNull(props.toString());
    }

    @Test
    void shouldReadOverridesFromYml() {
        ApplicationProperties props = ApplicationProperties.from("application",
            ParameterTool.fromMap(Map.of()), "/application-apptest.yml");

        Assertions.assertEquals(JobMode.OPTION_PRICES_ONLY, props.getJobMode());
        Assertions.assertTrue(props.isInterestRatesKafkaSnapshotEnabled());
        Assertions.assertTrue(props.isEnabledExtendedLogging());
    }
}
