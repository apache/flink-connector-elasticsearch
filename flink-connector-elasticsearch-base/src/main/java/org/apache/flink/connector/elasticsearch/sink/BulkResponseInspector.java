/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.function.SerializableFunction;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

/** Callback for inspecting a {@link BulkResponse}. */
@PublicEvolving
@FunctionalInterface
public interface BulkResponseInspector {

    /**
     * Callback to inspect a {@code response} in the context of its {@code request}. It may throw a
     * {@link org.apache.flink.util.FlinkRuntimeException} to indicate that the bulk failed
     * (partially).
     */
    void inspect(BulkRequest request, BulkResponse response);

    /**
     * Factory interface for creating a {@link BulkResponseInspector} in the context of a sink.
     * Allows obtaining a {@link org.apache.flink.metrics.MetricGroup} to capture custom metrics.
     */
    @PublicEvolving
    @FunctionalInterface
    interface BulkResponseInspectorFactory
            extends SerializableFunction<
                    BulkResponseInspectorFactory.InitContext, BulkResponseInspector> {

        /**
         * The interface exposes a subset of {@link
         * org.apache.flink.api.connector.sink2.Sink.InitContext}.
         */
        interface InitContext {

            /** Returns: The metric group of the surrounding writer. */
            MetricGroup metricGroup();
        }
    }
}
