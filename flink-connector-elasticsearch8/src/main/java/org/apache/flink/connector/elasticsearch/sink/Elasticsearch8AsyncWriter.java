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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Modified Elasticsearch8AsyncWriter Apache Flink's Async Sink Writer  that uses RetryableOperation to track and drop failed requests.
 * @param <InputT> type of Operations
 */

public class Elasticsearch8AsyncWriter<InputT> extends AsyncSinkWriter<InputT, RetryableOperation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncWriter.class);

    private final ElasticsearchAsyncClient esClient;
    private final boolean emergencyMode;
    private final int maxRetries;
    private boolean close = false;

    private final Counter numRecordsOutErrorsCounter;
    /**
     * A counter to track number of records that are returned by Elasticsearch as failed and then
     * retried by this writer.
     */
    private final Counter numRecordsSendPartialFailureCounter;
    /** A counter to track the number of bulk requests that are sent to Elasticsearch. */
    private final Counter numRequestSubmittedCounter;
    /** A counter to track the number of bulk requests that are skipped in emergency mode. */
    private final Counter numRecordsSkippedCounter;

    private final OperationSerializer operationSerializer;

    private static final FatalExceptionClassifier ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    new FatalExceptionClassifier(
                            err -> false, // No fatal network exceptions by default for this Sink
                            err -> new FlinkRuntimeException("Fatal Elasticsearch error", err)));

    public Elasticsearch8AsyncWriter(
            ElementConverter<InputT, RetryableOperation> elementConverter,
            WriterInitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
            Collection<BufferedRequestState<RetryableOperation>> state,
            boolean emergencyMode,
            int maxRetries
    ) {
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
        this.emergencyMode = emergencyMode;
        this.maxRetries = maxRetries;

        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendPartialFailureCounter = metricGroup.counter("numRecordsSendPartialFailure");
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
        this.numRecordsSkippedCounter = metricGroup.counter("numRecordsSkipped"); // New Metric
        this.operationSerializer = new OperationSerializer();
    }

    @Override
    protected void submitRequestEntries(
            List<RetryableOperation> requestEntries, ResultHandler<RetryableOperation> resultHandler) {

        numRequestSubmittedCounter.inc();
        LOG.debug("submitRequestEntries with {} items", requestEntries.size());

        List<RetryableOperation> toSend = new ArrayList<>(requestEntries.size());

        for (RetryableOperation op : requestEntries) {
            op.incrementAttempt(); // Increment attempt before sending
            if (op.getAttemptCount() == 2) {
                LOG.warn("Operation failed first time. Retrying... (sample op: {})", op.getOperation());
            }

            if (emergencyMode && op.getAttemptCount() > maxRetries) {
                LOG.warn("Emergency Mode: Dropping record after {} attempts. Operation: {}",
                        op.getAttemptCount(), op.getOperation());
                numRecordsSkippedCounter.inc();
                // In emergency mode, by not adding it to 'toSend', it counts as "Success" because we don't report it as failed later.
            } else if (!emergencyMode && op.getAttemptCount() > maxRetries) {
                String errorMsg = String.format(
                        "Retry limit (%d) exceeded in Non-Emergency Mode. Failing job to prevent infinite loop. Operation: %s",
                        maxRetries, op.getOperation());
                LOG.error(errorMsg);
                throw new FlinkRuntimeException(errorMsg);
            } else {
                toSend.add(op);
            }
        }

        if (toSend.isEmpty()) {
            resultHandler.complete();  // All items were dropped. Complete immediately.
            return;
        }

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (RetryableOperation operation : toSend) {
            br.operations(new BulkOperation(operation.getOperation().getBulkOperationVariant()));
        }

        esClient.bulk(br.build())
                .whenComplete(
                        (response, error) -> {
                            if (error != null) {
                                handleFailedRequest(toSend, resultHandler, error); // network error
                            } else if (response.errors()) {
                                handlePartiallyFailedRequest(toSend, resultHandler, response); // partial failures
                            } else {
                                handleSuccessfulRequest(resultHandler, response);
                            }
                        });
    }

    private void handleFailedRequest(
            List<RetryableOperation> requestEntries,
            ResultHandler<RetryableOperation> resultHandler,
            Throwable error) {
        LOG.warn(
                "The BulkRequest of {} operation(s) has completely failed due to: {}",
                requestEntries.size(),
                error.getMessage());
        LOG.debug("The BulkRequest has failed", error);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(error.getCause())) {
            resultHandler.retryForEntries(requestEntries);
        } else {
            Exception fatalException;
            if (error instanceof Exception) {
                fatalException = (Exception) error;
            } else {
                fatalException = new FlinkRuntimeException(error);
            }
            resultHandler.completeExceptionally(fatalException);
        }
    }

    private void handlePartiallyFailedRequest(
            List<RetryableOperation> requestEntries,
            ResultHandler<RetryableOperation> resultHandler,
            BulkResponse response) {
        LOG.debug("The BulkRequest has failed partially. Response: {}", response);
        ArrayList<RetryableOperation> failedItems = new ArrayList<>();
        int failureCount = 0;
        String firstErrorMessage = null;
        String firstErrorType = null;
        String firstErrorStatus = null;
        String indexName = null;
        String sampledFailedItemId = null;

        List<BulkResponseItem> items = response.items();
        for (int i = 0; i < items.size(); i++) {
            BulkResponseItem item = items.get(i);
            if (item.error() != null) {
                failedItems.add(requestEntries.get(i));
                failureCount++;
                if (firstErrorMessage == null) {
                    firstErrorMessage = item.error().reason();
                    firstErrorType = item.error().type();
                    firstErrorStatus = String.valueOf(item.status());
                    indexName = item.index();
                    sampledFailedItemId = item.id();
                }
            }
        }

        if (failureCount > 0) {
            LOG.info("BulkRequest had {} failed items out of {} for index {} with sample document ID {}. Error type: {}, status: {}, reason: '{}'",
                    failureCount, requestEntries.size(), indexName, sampledFailedItemId, firstErrorType, firstErrorStatus, firstErrorMessage);
        }

        numRecordsOutErrorsCounter.inc(failedItems.size());
        numRecordsSendPartialFailureCounter.inc(failedItems.size());
        resultHandler.retryForEntries(failedItems);
    }

    private void handleSuccessfulRequest(
            ResultHandler<RetryableOperation> resultHandler, BulkResponse response) {
        LOG.debug(
                "The BulkRequest of {} operation(s) completed successfully. It took {}ms",
                response.items().size(),
                response.took());
        resultHandler.complete();
    }

    private boolean isRetryable(Throwable error) {
        return !ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER.isFatal(error, getFatalExceptionCons());
    }

    @Override
    protected long getSizeInBytes(RetryableOperation requestEntry) {
        return operationSerializer.size(requestEntry.getOperation()) + 4; // We add constant overhead (4 bytes for int) to size
    }

    @Override
    public void close() {
        if (!close) {
            close = true;
            esClient.shutdown();
        }
    }
}
