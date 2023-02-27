/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.config.operator;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Adds sink capabilities to operator definition (for Sinks based on old Flink API).
 *
 * @param <T> Sink event type
 */
@FunctionalInterface
public interface SinkDefinition<T> extends OperatorDefinition {
    /**
     * Builds Flink {@link SinkFunction}
     */
    SinkFunction<T> buildSink();
}