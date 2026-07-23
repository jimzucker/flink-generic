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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Exercises the pure parsing/precedence helpers behind all operator config binding.
 */
class OperatorPropertiesUtilsTest {

    @Test
    void stripPrefixesRemovesMatchingPrefix() {
        Map<String, Object> result = OperatorPropertiesUtils.stripPrefixes(
            Map.of("aws.region", "us-east-1"), Map.of("aws", 0));

        Assertions.assertEquals("us-east-1", result.get("region"));
        Assertions.assertFalse(result.containsKey("aws.region"));
    }

    @Test
    void stripPrefixesKeepsUnprefixedKeysUnchanged() {
        Map<String, Object> result = OperatorPropertiesUtils.stripPrefixes(
            Map.of("bootstrap.servers", "localhost:9092"), Map.of("aws", 0));

        Assertions.assertEquals("localhost:9092", result.get("bootstrap.servers"));
    }

    @Test
    void stripPrefixesResolvesCollisionByLowestRank() {
        // Both keys strip to "topic"; the lower-ranked prefix (source=0) must win over shared=1.
        Map<String, String> params = new LinkedHashMap<>();
        params.put("source.topic", "primary");
        params.put("shared.topic", "fallback");
        Map<String, Integer> ranked = new LinkedHashMap<>();
        ranked.put("source", 0);
        ranked.put("shared", 1);

        Map<String, Object> result = OperatorPropertiesUtils.stripPrefixes(params, ranked);

        Assertions.assertEquals("primary", result.get("topic"));
    }

    @Test
    void convertEnvVariablesEmitsDottedAndCamelForms() {
        Map<String, Object> result = OperatorPropertiesUtils.convertEnvVariables(Map.of("MY_PROP", "v"));

        Assertions.assertEquals("v", result.get("my.prop"));
        Assertions.assertEquals("v", result.get("myProp"));
    }

    @Test
    void buildEnvKeyStripsPrefixOrReturnsNull() {
        Assertions.assertEquals("REGION", OperatorPropertiesUtils.buildEnvKey(Set.of("AWS_"), "AWS_REGION"));
        Assertions.assertNull(OperatorPropertiesUtils.buildEnvKey(Set.of("AWS_"), "OTHER_KEY"));
    }

    @Test
    void removeNullableEntriesDropsOnlyNullSentinels() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("keep", "value");
        map.put("drop", "NULL");

        OperatorPropertiesUtils.removeNullableEntries(map);

        Assertions.assertTrue(map.containsKey("keep"));
        Assertions.assertFalse(map.containsKey("drop"));
    }
}
