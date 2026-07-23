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

package com.ness.flink.config.channel.kafka.msk;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AwsGlueSchemaNamingStrategyTest {

    private final AwsGlueSchemaNamingStrategy strategy = new AwsGlueSchemaNamingStrategy();

    @Test
    void shouldSuffixValueForTopicOnlyOverload() {
        Assertions.assertEquals("orders-value", strategy.getSchemaName("orders"));
    }

    @Test
    void shouldReturnTopicNameForDataOverload() {
        Assertions.assertEquals("orders", strategy.getSchemaName("orders", new Object()));
    }

    @Test
    void shouldSuffixKeyWhenIsKeyTrue() {
        Assertions.assertEquals("orders-key", strategy.getSchemaName("orders", new Object(), true));
    }

    @Test
    void shouldSuffixValueWhenIsKeyFalse() {
        Assertions.assertEquals("orders-value", strategy.getSchemaName("orders", new Object(), false));
    }
}
