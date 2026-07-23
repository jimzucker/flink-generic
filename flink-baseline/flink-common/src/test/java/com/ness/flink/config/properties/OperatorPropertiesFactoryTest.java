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

package com.ness.flink.config.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Exercises the shared property-loading mechanics of {@link OperatorPropertiesFactory}
 * (defaults from the classpath application.yml, prefixed ParameterTool override precedence,
 * and raw-values population) through the {@link AwsProperties} holder.
 */
class OperatorPropertiesFactoryTest {

    @Test
    void shouldLoadDefaultsFromClasspathYaml() {
        AwsProperties properties = AwsProperties.from(ParameterTool.fromMap(Map.of()));

        Assertions.assertEquals("us-east-1", properties.getRegion());
    }

    @Test
    void shouldPopulateRawValuesWithPrefixStrippedKeys() {
        AwsProperties properties = AwsProperties.from(ParameterTool.fromMap(Map.of()));

        // glue.* entries from the "aws" section are exposed as raw values (prefix stripped).
        Assertions.assertEquals("poc-msk-shema-registry",
            properties.getRawValues().get("glue.registry.name"));
    }

    @Test
    void shouldOverrideYamlDefaultWithPrefixedParameter() {
        AwsProperties properties = AwsProperties.from(
            ParameterTool.fromMap(Map.of("aws.region", "eu-west-1")));

        Assertions.assertEquals("eu-west-1", properties.getRegion());
    }

    @Test
    void shouldNotApplyParameterFromDifferentPrefix() {
        // A parameter scoped to a different name must not leak into aws properties.
        AwsProperties properties = AwsProperties.from(
            ParameterTool.fromMap(Map.of("redis.region", "eu-west-1")));

        Assertions.assertEquals("us-east-1", properties.getRegion());
    }
}
