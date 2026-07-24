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

package com.ness.flink.config.aws;

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricsBuilderTest {

    @Test
    void shouldReplaceDotsInOperatorNameWhenRegistering() {
        MetricGroup root = mock(MetricGroup.class);
        MetricGroup child = mock(MetricGroup.class);
        when(root.addGroup("kinesisanalytics", "my_operator_name")).thenReturn(child);

        MetricGroup result = MetricsBuilder.register(root, "my.operator.name");

        Assertions.assertSame(child, result);
        verify(root).addGroup("kinesisanalytics", "my_operator_name");
    }

    @Test
    void shouldRegisterHistogramWithHistogramPostfix() {
        MetricGroup root = mock(MetricGroup.class);
        MetricGroup child = mock(MetricGroup.class);
        when(root.addGroup(eq("kinesisanalytics"), any())).thenReturn(child);
        DropwizardHistogramWrapper wrapper = mock(DropwizardHistogramWrapper.class);
        when(child.histogram(eq("latencyHistogram"), any(DropwizardHistogramWrapper.class))).thenReturn(wrapper);

        DropwizardHistogramWrapper result = MetricsBuilder.histogram(root, "op", "latency");

        Assertions.assertSame(wrapper, result);
        verify(child).histogram(eq("latencyHistogram"), any(DropwizardHistogramWrapper.class));
    }

    @Test
    void shouldRegisterGaugeWithGaugePostfix() {
        MetricGroup root = mock(MetricGroup.class);
        MetricGroup child = mock(MetricGroup.class);
        when(root.addGroup(eq("kinesisanalytics"), any())).thenReturn(child);
        Gauge<Integer> gauge = () -> 5;

        MetricsBuilder.gauge(root, "op", "latency", gauge);

        verify(child).gauge("latencyGauge", gauge);
    }

    @Test
    void shouldRegisterCounterWithCounterPostfix() {
        MetricGroup root = mock(MetricGroup.class);
        MetricGroup child = mock(MetricGroup.class);
        when(root.addGroup(eq("kinesisanalytics"), any())).thenReturn(child);

        MetricsBuilder.counter(root, "op", "records");

        verify(child).counter("recordsCounter");
    }
}
