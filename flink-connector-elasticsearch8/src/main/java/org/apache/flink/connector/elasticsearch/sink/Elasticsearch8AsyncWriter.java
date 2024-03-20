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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch8AsyncWriter Apache Flink's Async Sink Writer that submits Operations into an
 * Elasticsearch cluster.
 *
 * @param <InputT> type of Operations
 */
public class Elasticsearch8AsyncWriter<InputT> extends AsyncSinkWriter<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncWriter.class);

    private final ElasticsearchAsyncClient esClient;

    private boolean close = false;

    private final Counter numRecordsOutErrorsCounter;

    private static final FatalExceptionClassifier ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    new FatalExceptionClassifier(
                            err ->
                                    err instanceof NoRouteToHostException
                                            || err instanceof ConnectException,
                            err ->
                                    new FlinkRuntimeException(
                                            "Could not connect to Elasticsearch cluster using the provided hosts",
                                            err)));

    public Elasticsearch8AsyncWriter(
            ElementConverter<InputT, Operation> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String username,
            String password,
            String certificateFingerprint,
            List<HttpHost> httpHosts,
            List<Header> headers,
            Collection<BufferedRequestState<Operation>> state) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                state);

        this.esClient =
                new NetworkConfig(httpHosts, username, password, headers, certificateFingerprint)
                        .create();
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
    }

    @Override
    protected void submitRequestEntries(
            List<Operation> requestEntries, Consumer<List<Operation>> requestResult) {
        LOG.debug("submitRequestEntries with {} items", requestEntries.size());

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Operation operation : requestEntries) {
            br.operations(new BulkOperation(operation.getBulkOperationVariant()));
        }

        esClient.bulk(br.build())
                .whenComplete(
                        (response, error) -> {
                            if (error != null) {
                                handleFailedRequest(requestEntries, requestResult, error);
                            } else if (response.errors()) {
                                handlePartiallyFailedRequest(
                                        requestEntries, requestResult, response);
                            } else {
                                handleSuccessfulRequest(requestResult, response);
                            }
                        });
    }

    private void handleFailedRequest(
            List<Operation> requestEntries,
            Consumer<List<Operation>> requestResult,
            Throwable error) {
        LOG.debug("The BulkRequest of {} operation(s) has failed.", requestEntries.size());
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(error.getCause())) {
            requestResult.accept(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            List<Operation> requestEntries,
            Consumer<List<Operation>> requestResult,
            BulkResponse response) {
        ArrayList<Operation> failedItems = new ArrayList<>();
        for (int i = 0; i < response.items().size(); i++) {
            if (response.items().get(i).error() != null) {
                numRecordsOutErrorsCounter.inc();
                failedItems.add(requestEntries.get(i));
            }
        }

        LOG.debug(
                "The BulkRequest with {} operation(s) has {} failure(s). It took {}ms",
                requestEntries.size(),
                failedItems.size(),
                response.took());
        requestResult.accept(failedItems);
    }

    private void handleSuccessfulRequest(
            Consumer<List<Operation>> requestResult, BulkResponse response) {
        LOG.debug(
                "The BulkRequest of {} operation(s) completed successfully. It took {}ms",
                response.items().size(),
                response.took());
        requestResult.accept(Collections.emptyList());
    }

    private boolean isRetryable(Throwable error) {
        return !ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER.isFatal(error, getFatalExceptionCons());
    }

    @Override
    protected long getSizeInBytes(Operation requestEntry) {
        return new OperationSerializer().size(requestEntry);
    }

    @Override
    public void close() {
        if (!close) {
            close = true;
            esClient.shutdown();
        }
    }
}
