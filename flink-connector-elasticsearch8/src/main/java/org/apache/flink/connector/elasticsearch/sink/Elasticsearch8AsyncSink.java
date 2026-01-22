/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Elasticsearch8AsyncSink Apache Flink's Async Sink that submits Retryable operations into an Elasticsearch
 * cluster.
 *
 * @param <InputT> type of records that will be converted into {@link RetryableOperation} see {@link
 *         Elasticsearch8AsyncSinkBuilder} on how to construct valid instances
 */
public class Elasticsearch8AsyncSink<InputT> extends AsyncSinkBase<InputT, RetryableOperation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncSink.class);

    @VisibleForTesting
    protected final NetworkConfig networkConfig;
    private final boolean emergencyMode;
    private final int maxRetries;

    protected Elasticsearch8AsyncSink(
            ElementConverter<InputT, RetryableOperation> converter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInByte,
            NetworkConfig networkConfig,
            boolean emergencyMode,
            int maxRetries) {
        super(
                converter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInByte);

        this.networkConfig = networkConfig;
        this.emergencyMode = emergencyMode;
        this.maxRetries = maxRetries;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<RetryableOperation>> createWriter(
            WriterInitContext context) {
        return new Elasticsearch8AsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                networkConfig,
                Collections.emptyList(),
                emergencyMode,
                maxRetries);
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<RetryableOperation>> restoreWriter(
            WriterInitContext context,
            Collection<BufferedRequestState<RetryableOperation>> recoveredState) {
        return new Elasticsearch8AsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                networkConfig,
                recoveredState,
                emergencyMode,
                maxRetries);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<RetryableOperation>> getWriterStateSerializer() {
        return new Elasticsearch8AsyncSinkSerializer();
    }
}
