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

package com.ness.flink.assigner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WithMetricAssignerTest {

    @Test
    void createdAssignerDelegatesTimestampAndRecordsLatency() {
        // MetricsBuilder registers under a "kinesisanalytics" child group; stub the chain it walks.
        MetricGroup root = mock(MetricGroup.class);
        MetricGroup child = mock(MetricGroup.class);
        when(root.addGroup(eq("kinesisanalytics"), any())).thenReturn(child);
        DropwizardHistogramWrapper histogram = mock(DropwizardHistogramWrapper.class);
        when(child.histogram(any(), any(DropwizardHistogramWrapper.class))).thenReturn(histogram);

        TimestampAssignerSupplier.Context context = mock(TimestampAssignerSupplier.Context.class);
        when(context.getMetricGroup()).thenReturn(root);

        SerializableTimestampAssigner<String> delegate = (element, recordTimestamp) -> 4242L;
        WithMetricAssigner<String> supplier = new WithMetricAssigner<>("my.operator", delegate);

        TimestampAssigner<String> assigner = supplier.createTimestampAssigner(context);
        long extracted = assigner.extractTimestamp("event", 0L);

        Assertions.assertEquals(4242L, extracted, "must return the wrapped assigner's timestamp");
        verify(histogram).update(anyLong());
    }
}
