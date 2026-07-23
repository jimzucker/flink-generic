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

import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.snapshot.context.properties.ContextProperties;
import com.ness.flink.snapshot.context.rest.RestBased;
import com.ness.flink.window.generator.GeneratorType;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class ContextServiceProviderTest {

    private static WatermarkProperties watermark() {
        return WatermarkProperties.from(ParameterTool.fromMap(Map.of("watermark.windowSizeMs", "10000")));
    }

    @Test
    void shouldCreateWindowBasedServiceForBasicGenerator() {
        ContextProperties properties = new ContextProperties();
        // BASIC is the default generator type.

        ContextService service = ContextServiceProvider.create(properties, watermark());

        Assertions.assertInstanceOf(WindowBased.class, service);
    }

    @Test
    void shouldCreateRestBasedServiceForRestGenerator() {
        ContextProperties properties = new ContextProperties();
        properties.setGeneratorType(GeneratorType.REST);
        properties.setServiceUrl("http://localhost:9999");

        ContextService service = ContextServiceProvider.create(properties, watermark());

        Assertions.assertInstanceOf(RestBased.class, service);
    }
}
