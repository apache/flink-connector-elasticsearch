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

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private static final Set<Integer> ELASTICSEARCH_NON_RETRYABLE_STATUS =
            new HashSet<>(Arrays.asList(400, 404));

    public Elasticsearch8AsyncWriter(
            ElementConverter<InputT, Operation> elementConverter,
            WriterInitContext context,
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
            List<Operation> requestEntries, ResultHandler<Operation> resultHandler) {
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
                                handleFailedRequest(requestEntries, resultHandler, error);
                            } else if (response.errors()) {
                                handlePartiallyFailedRequest(
                                        requestEntries, resultHandler, response);
                            } else {
                                handleSuccessfulRequest(resultHandler, response);
                            }
                        });
    }

    private void handleFailedRequest(
            List<Operation> requestEntries,
            ResultHandler<Operation> resultHandler,
            Throwable error) {
        LOG.warn(
                "The BulkRequest of {} operation(s) has failed due to: {}",
                requestEntries.size(),
                error.getMessage());
        LOG.debug("The BulkRequest has failed", error);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(error.getCause())) {
            resultHandler.retryForEntries(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            List<Operation> requestEntries,
            ResultHandler<Operation> resultHandler,
            BulkResponse response) {
        LOG.debug("The BulkRequest has failed partially. Response: {}", response);

        ArrayList<Operation> failedItemsToRetry = new ArrayList<>();
        int totalFailedItems = 0;
        FlinkRuntimeException nonRetryableException = null;

        for (int i = 0; i < response.items().size(); i++) {
            BulkResponseItem responseItem = response.items().get(i);
            if (responseItem.error() != null) {
                totalFailedItems++;
                if (isOperationRetryable(responseItem.status())) {
                    failedItemsToRetry.add(requestEntries.get(i));
                } else {
                    LOG.error(
                            "Failed to process non-retryable operation: {}, response: {}",
                            requestEntries.get(i),
                            responseItem);
                    nonRetryableException =
                            new FlinkRuntimeException(
                                    "Failed to process non-retryable operation, reason=%s"
                                            + responseItem.error().reason());
                    break;
                }
            }
        }

        numRecordsOutErrorsCounter.inc(totalFailedItems);
        LOG.info(
                "The BulkRequest with {} operation(s) has {} failure(s), {} retryable. It took {}ms",
                requestEntries.size(),
                totalFailedItems,
                failedItemsToRetry.size(),
                response.took());

        if (nonRetryableException != null) {
            resultHandler.completeExceptionally(nonRetryableException);
        } else {
            numRecordsSendPartialFailureCounter.inc(failedItemsToRetry.size());
            resultHandler.retryForEntries(failedItemsToRetry);
        }
    }

    private void handleSuccessfulRequest(
            ResultHandler<Operation> resultHandler, BulkResponse response) {
        LOG.debug(
                "The BulkRequest of {} operation(s) completed successfully. It took {}ms",
                response.items().size(),
                response.took());
        resultHandler.complete();
    }

    private boolean isRetryable(Throwable error) {
        return !ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER.isFatal(error, getFatalExceptionCons());
    }

    /** Given the response status, check if an operation is retryable. */
    private static boolean isOperationRetryable(int status) {
        return !ELASTICSEARCH_NON_RETRYABLE_STATUS.contains(status);
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
