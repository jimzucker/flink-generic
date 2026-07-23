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

package com.ness.flink.config.environment;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Verifies {@link EnvironmentFactory#from} applies the environment configuration branches
 * (watermark interval, buffer timeout, object reuse, global job parameters) that silently affect
 * every job. Uses the default {@code localDev=false} path (plain local env, no web UI/port).
 */
class EnvironmentFactoryTest {

    private static StreamExecutionEnvironment env(Map<String, String> overrides) {
        return EnvironmentFactory.from(ParameterTool.fromMap(overrides));
    }

    @Test
    void shouldApplyAutoWatermarkIntervalFromConfig() {
        StreamExecutionEnvironment env = env(Map.of());

        Assertions.assertNotNull(env);
        // 500 comes from the test application.yml environment section
        Assertions.assertEquals(500L, env.getConfig().getAutoWatermarkInterval());
    }

    @Test
    void shouldRegisterGlobalJobParameters() {
        StreamExecutionEnvironment env = env(Map.of("foo", "bar"));

        Assertions.assertNotNull(env.getConfig().getGlobalJobParameters());
        Assertions.assertEquals("bar",
            env.getConfig().getGlobalJobParameters().toMap().get("foo"));
    }

    @Test
    void shouldEnableObjectReuseWhenConfigured() {
        StreamExecutionEnvironment env = env(Map.of("environment.enabledObjectReuse", "true"));

        Assertions.assertTrue(env.getConfig().isObjectReuseEnabled());
    }

    @Test
    void shouldOverrideAutoWatermarkInterval() {
        StreamExecutionEnvironment env = env(Map.of("environment.autoWatermarkInterval", "250"));

        Assertions.assertEquals(250L, env.getConfig().getAutoWatermarkInterval());
    }

    @Test
    void shouldApplyBufferTimeout() {
        StreamExecutionEnvironment env = env(Map.of("environment.bufferTimeoutMs", "75"));

        Assertions.assertEquals(75L, env.getBufferTimeout());
    }
}
