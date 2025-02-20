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
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    /**
     * A counter to track number of records that are returned by Elasticsearch as failed and then
     * retried by this writer.
     */
    private final Counter numRecordsSendPartialFailureCounter;
    /** A counter to track the number of bulk requests that are sent to Elasticsearch. */
    private final Counter numRequestSubmittedCounter;

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

    private static final Set<Integer> ELASTICSEARCH_NON_RETRYBLE_STATUS =
            new HashSet<>(Arrays.asList(400, 404));

    public Elasticsearch8AsyncWriter(
            ElementConverter<InputT, Operation> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
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

        this.esClient = networkConfig.createEsClient();
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendPartialFailureCounter =
                metricGroup.counter("numRecordsSendPartialFailure");
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
    }

    @Override
    protected void submitRequestEntries(
            List<Operation> requestEntries, Consumer<List<Operation>> requestResult) {
        numRequestSubmittedCounter.inc();
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
        LOG.warn(
                "The BulkRequest of {} operation(s) has failed due to: {}",
                requestEntries.size(),
                error.getMessage());
        LOG.debug("The BulkRequest has failed", error);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(error.getCause())) {
            requestResult.accept(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            List<Operation> requestEntries,
            Consumer<List<Operation>> requestResult,
            BulkResponse response) {
        LOG.debug("The BulkRequest has failed partially. Response: {}", response);
        long failedItemsCount =
                response.items().stream().filter(item -> item.error() != null).count();
        numRecordsOutErrorsCounter.inc(failedItemsCount);
        numRecordsSendPartialFailureCounter.inc(failedItemsCount);
        LOG.info(
                "The BulkRequest with {} operation(s) has {} failure(s). It took {}ms",
                requestEntries.size(),
                failedItemsCount,
                response.took());

        ArrayList<Operation> failedItemsToRetry = new ArrayList<>();
        for (int i = 0; i < response.items().size(); i++) {
            BulkResponseItem responseItem = response.items().get(i);
            if (responseItem.error() != null) {
                if (isOperationRetryable(responseItem.status())) {
                    failedItemsToRetry.add(requestEntries.get(i));
                } else {
                    LOG.error(
                            "Failed to process non-retriable operation: {}, response: {}",
                            requestEntries.get(i),
                            responseItem);
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Failed to process non-retriable operation, reason=%s",
                                    responseItem.error().reason()));
                }
            }
        }

        requestResult.accept(failedItemsToRetry);
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

    /** Given the response status, check if an operation is retryable. */
    private static boolean isOperationRetryable(int status) {
        return !ELASTICSEARCH_NON_RETRYBLE_STATUS.contains(status);
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
