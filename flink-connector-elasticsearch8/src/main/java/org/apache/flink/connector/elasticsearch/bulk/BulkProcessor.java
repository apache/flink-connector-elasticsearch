/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.elasticsearch.bulk;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.util.ObjectBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Bulk processor which using @ElasticsearchAsyncClient.
 *
 * @param <Context>
 */
public class BulkProcessor<Context> implements AutoCloseable {

    private static final Log logger = LogFactory.getLog(BulkProcessor.class);

    // Instance counter, to name the flush thread if we create one
    private static final AtomicInteger idCounter = new AtomicInteger();

    // Configuration
    private final ElasticsearchAsyncClient client;
    private final @Nullable BulkRequest globalSettings;
    private final int maxConcurrentRequests;
    private final long maxSize;
    private final int maxOperations;
    private final @Nullable BulkListener<Context> listener;
    private final Long flushIntervalMillis;

    private @Nullable ScheduledFuture<?> flushTask;
    private @Nullable ScheduledExecutorService scheduler;

    // Current state
    private List<BulkOperation> operations = new ArrayList<>();
    private List<Context> contexts = null; // Created on demand
    private long currentSize;
    private int requestsInFlightCount;
    private volatile boolean isClosed = false;

    // Synchronization objects
    private final ReentrantLock lock = new ReentrantLock();
    private final FnCondition addCondition = new FnCondition(lock, this::isOverTheLimit);
    private final FnCondition sendRequestCondition = new FnCondition(lock, this::canSendRequest);
    private final FnCondition closeCondition = new FnCondition(lock, this::closedAndFlushed);

    private static class RequestExecution<Context> {
        public final long id;
        public final BulkRequest request;
        public final List<Context> contexts;
        public final CompletionStage<BulkResponse> futureResponse;

        RequestExecution(
                long id,
                BulkRequest request,
                List<Context> contexts,
                CompletionStage<BulkResponse> futureResponse) {
            this.id = id;
            this.request = request;
            this.contexts = contexts;
            this.futureResponse = futureResponse;
        }
    }

    private BulkProcessor(Builder<Context> builder) {
        int processorId = idCounter.incrementAndGet();
        this.client = builder.client;
        this.globalSettings = builder.globalSettings;
        this.maxConcurrentRequests = builder.maxConcurrentRequests;
        this.maxSize = builder.bulkSize < 0 ? 1000 : builder.bulkSize;
        this.maxOperations =
                builder.bulkOperations < 0 ? 1000 : builder.bulkOperations;
        this.listener = builder.listener;
        this.flushIntervalMillis = builder.flushIntervalMillis;

        if (flushIntervalMillis != null) {
            long flushInterval = flushIntervalMillis;

            // Create a scheduler if needed
            ScheduledExecutorService scheduler = builder.scheduler;
            if (scheduler == null) {
                scheduler =
                        Executors.newSingleThreadScheduledExecutor(
                                (r) -> {
                                    Thread t = Executors.defaultThreadFactory().newThread(r);
                                    t.setName("bulk-elasticsearch-flusher#" + processorId);
                                    t.setDaemon(true);
                                    return t;
                                });

                // Keep it, we'll have to close it.
                this.scheduler = scheduler;
            }

            this.flushTask =
                    scheduler.scheduleWithFixedDelay(
                            this::failsafeFlush,
                            flushInterval,
                            flushInterval,
                            TimeUnit.MILLISECONDS);
        }
    }

    // ----- Getters

    /** The configured max operations to buffer in a single bulk request. */
    public int maxOperations() {
        return this.maxOperations;
    }

    /**
     * The configured maximum size in bytes for a bulk request. Operations are added to the request
     * until adding an operation leads the request to exceed this siz.
     */
    public long maxSize() {
        return this.maxSize;
    }

    /** The configured maximum number of concurrent request sent to Elasticsearch. */
    public int maxConcurrentRequests() {
        return this.maxConcurrentRequests;
    }

    /** The configured flush period. */
    public Duration flushInterval() {
        if (this.flushIntervalMillis != null) {
            return Duration.ofMillis(flushIntervalMillis);
        } else {
            return null;
        }
    }

    /** The number of operations that have been buffered, waiting to be sent. */
    public int pendingOperations() {
        List<BulkOperation> operations = this.operations;
        return operations == null ? 0 : operations.size();
    }

    /** The size in bytes of operations that have been buffered, waiting to be sent. */
    public long pendingOperationsSize() {
        return this.currentSize;
    }

    /** The number of in flight bulk requests. */
    public int pendingRequests() {
        return this.requestsInFlightCount;
    }

    // ----- Statistics

    /**
     * Statistics: the number of operations that were added to this ingester since it was created.
     */
    public long operationsCount() {
        return this.addCondition.invocations();
    }

    /**
     * Statistics: the number of operations that had to wait before being added because the
     * operation buffer was full and the number of http requests in flight exceeded the configured
     * maximum number.
     *
     * @see Builder#maxConcurrentRequests
     * @see Builder#maxOperations
     * @see Builder#maxSize
     */
    public long operationContentionsCount() {
        return this.addCondition.contentions();
    }

    /**
     * Statistics: the number of bulk requests that were produced by this ingester since it was
     * created.
     */
    public long requestCount() {
        return this.sendRequestCondition.invocations();
    }

    /**
     * Statistics: the number of bulk requests that could not be sent immediately because the number
     * of http requests in flight exceeded the configured maximum number.
     *
     * @see Builder#maxConcurrentRequests
     */
    public long requestContentionsCount() {
        return this.sendRequestCondition.contentions();
    }

    // ----- Predicates for the condition variables

    private boolean canSendRequest() {
        return requestsInFlightCount < maxConcurrentRequests;
    }

    private boolean isOverTheLimit() {
        return currentSize < maxSize && operations.size() < maxOperations;
    }

    private boolean closedAndFlushed() {
        return isClosed && operations.isEmpty() && requestsInFlightCount == 0;
    }

    // ----- BulkProcessor logic

    private BulkRequest.Builder newRequest() {
        BulkRequest.Builder result = new BulkRequest.Builder();

        if (this.globalSettings != null) {
            BulkRequest settings = this.globalSettings;
            result.index(settings.index())
                    .pipeline(settings.pipeline())
                    .refresh(settings.refresh())
                    .requireAlias(settings.requireAlias())
                    .routing(settings.routing())
                    .sourceExcludes(settings.sourceExcludes())
                    .sourceIncludes(settings.sourceIncludes())
                    .source(settings.source())
                    .timeout(settings.timeout())
                    .waitForActiveShards(settings.waitForActiveShards());
        }

        return result;
    }

    private void failsafeFlush() {
        try {
            flush();
        } catch (Throwable thr) {
            // Log the error and continue
            logger.error("Error in background flush", thr);
        }
    }

    public void flush() {
        RequestExecution<Context> exec =
                sendRequestCondition.whenReadyIf(
                        () -> {
                            // May happen on manual and periodic flushes
                            return !operations.isEmpty();
                        },
                        () -> {
                            // Build the request
                            BulkRequest request = newRequest().operations(operations).build();
                            List<Context> requestContexts =
                                    contexts == null
                                            ? Collections.nCopies(operations.size(), null)
                                            : contexts;

                            // Prepare for next round
                            operations = new ArrayList<>();
                            contexts = null;
                            currentSize = 0;
                            addCondition.signalIfReady();

                            long id = sendRequestCondition.invocations();

                            if (listener != null) {
                                listener.beforeBulk(id, request, requestContexts);
                            }

                            CompletionStage<BulkResponse> result = client.bulk(request);
                            requestsInFlightCount++;

                            if (listener == null) {
                                // No need to keep the request around, it can be GC'ed
                                request = null;
                            }

                            return new RequestExecution<>(id, request, requestContexts, result);
                        });

        if (exec != null) {
            // A request was actually sent
            exec.futureResponse
                    .handle(
                            (resp, thr) -> {
                                sendRequestCondition.signalIfReadyAfter(
                                        () -> {
                                            requestsInFlightCount--;
                                            closeCondition.signalAllIfReady();
                                        });

                                if (resp != null) {
                                    // Success
                                    if (listener != null) {
                                        listener.afterBulk(
                                                exec.id, exec.request, exec.contexts, resp);
                                    }
                                } else {
                                    // Failure
                                    if (listener != null) {
                                        listener.afterBulk(
                                                exec.id, exec.request, exec.contexts, thr);
                                    }
                                }
                                return null;
                            })
                    .whenComplete(
                            (a, thr) -> {
                                if (thr != null) {
                                    logger.debug("Flush failed " + thr.getMessage());
                                } else {
                                    logger.debug("Flush success");
                                }
                            });
        }
    }

    public void add(BulkOperation operation, Context context) {
        if (isClosed) {
            throw new IllegalStateException("BulkProcessor has been closed");
        }

        InternalOperation ingestOp = InternalOperation.of(operation, client._jsonpMapper());

        addCondition.whenReady(
                () -> {
                    if (context != null) {
                        // Lazily build the context list
                        if (contexts == null) {
                            int size = operations.size();
                            if (size == 0) {
                                contexts = new ArrayList<>();
                            } else {
                                contexts = new ArrayList<>(Collections.nCopies(size, null));
                            }
                        }
                        contexts.add(context);
                    }
                    logger.debug("add operation: " + ingestOp.operation());
                    operations.add(ingestOp.operation());
                    currentSize += ingestOp.size();

                    if (!isOverTheLimit()) {
                        logger.debug("Over the limit and flush now...");
                        flush();
                    }
                });
    }

    public void add(BulkOperation operation) {
        add(operation, null);
    }

    public void add(Function<BulkOperation.Builder, ObjectBuilder<BulkOperation>> f) {
        add(f.apply(new BulkOperation.Builder()).build(), null);
    }

    public void add(
            Function<BulkOperation.Builder, ObjectBuilder<BulkOperation>> f, Context context) {
        add(f.apply(new BulkOperation.Builder()).build(), context);
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }

        isClosed = true;
        // Flush buffered operations
        flush();
        // and wait for all requests to be completed
        closeCondition.whenReady(() -> {});

        if (flushTask != null) {
            flushTask.cancel(false);
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    // ----------------------------------------------------------------------------------------------------

    public static <Context> BulkProcessor<Context> of(
            Function<Builder<Context>, Builder<Context>> f) {
        return f.apply(new Builder<>()).build();
    }

    public static <Context> BulkProcessor.Builder<Context> ofBuilder(
            Function<Builder<Context>, Builder<Context>> f) {
        return f.apply(new Builder<>());
    }

    /**
     * Builder.
     *
     * @param <Context>
     */
    public static class Builder<Context> implements ObjectBuilder<BulkProcessor<Context>> {
        private ElasticsearchAsyncClient client;
        private BulkRequest globalSettings;
        private int bulkOperations = 1000;
        private long bulkSize = 5 * 1024 * 1024;
        private int maxConcurrentRequests = 1;
        private Long flushIntervalMillis = null;
        private BulkListener<Context> listener;
        private ScheduledExecutorService scheduler;

        public Builder<Context> client(ElasticsearchAsyncClient client) {
            this.client = client;
            return this;
        }

        public Builder<Context> client(ElasticsearchClient client) {
            TransportOptions options = client._transportOptions();
            if (options == client._transport().options()) {
                options = null;
            }
            return client(new ElasticsearchAsyncClient(client._transport(), options));
        }

        /**
         * Sets when to flush a new bulk request based on the number of operations currently added.
         * Defaults to {@code 1000}. Can be set to {@code -1} to disable it.
         */
        public Builder<Context> maxOperations(int count) {
            this.bulkOperations = count;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size in bytes of actions currently
         * added. A request is sent once that size has been exceeded. Defaults to 5 megabytes. Can
         * be set to {@code -1} to disable it.
         */
        public Builder<Context> maxSize(long bytes) {
            this.bulkSize = bytes;
            return this;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 1 means 1
         * concurrent request is allowed to be executed while accumulating new bulk requests.
         * Defaults to {@code 1}.
         */
        public Builder<Context> maxConcurrentRequests(int max) {
            this.maxConcurrentRequests = max;
            return this;
        }

        /**
         * Sets an interval flushing any bulk actions pending if the interval passes. Defaults to
         * not set.
         *
         * <p>Flushing is still subject to the maximum number of requests set with {@link
         * #maxConcurrentRequests}.
         */
        public Builder<Context> flushInterval(long value, TimeUnit unit) {
            if (value > 0) {
                this.flushIntervalMillis = unit.toMillis(value);
            }
            return this;
        }

        /**
         * Sets an interval flushing any bulk actions pending if the interval passes. Defaults to
         * not set.
         *
         * <p>Flushing is still subject to the maximum number of requests set with {@link
         * #maxConcurrentRequests}.
         */
        public Builder<Context> flushInterval(
                long value, TimeUnit unit, ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return flushInterval(value, unit);
        }

        public Builder<Context> listener(BulkListener<Context> listener) {
            this.listener = listener;
            return this;
        }

        /**
         * Sets global bulk request settings that will be applied to all requests sent by the
         * ingester.
         */
        public Builder<Context> globalSettings(BulkRequest.Builder settings) {
            if (settings != null) {
                // Set required field
                this.globalSettings = settings.operations(Collections.emptyList()).build();
            } else {
                this.globalSettings = null;
            }
            return this;
        }

        /** Sets global bulk request settings that will be applied to all bulk requests. */
        public Builder<Context> globalSettings(
                Function<BulkRequest.Builder, BulkRequest.Builder> fn) {
            return globalSettings(fn.apply(new BulkRequest.Builder()));
        }

        @Override
        public BulkProcessor<Context> build() {
            return new BulkProcessor<>(this);
        }
    }

    /**
     * A listener that is called by a {@link BulkProcessor} to allow monitoring requests sent and
     * their result.
     *
     * @param <Context> application-defined contextual data that can be associated to a bulk
     *     operation.
     */
    public interface BulkListener<Context> extends Serializable {

        /**
         * Called before a bulk request is sent. <b>Note:</b> documents in {@code request}
         * operations have been converted to {@link co.elastic.clients.util.BinaryData}.
         *
         * @param executionId the id of this request, unique for the {@link BulkProcessor} that
         *     created it.
         * @param request the bulk request that will be sent, with documents in binary form.
         * @param contexts application-defined data that was passed in {@link
         *     BulkProcessor#add(BulkOperation, Object)}.
         */
        void beforeBulk(long executionId, BulkRequest request, List<Context> contexts);

        /**
         * Called after a bulk request has been processed. Elasticsearch accepted the request, but
         * {@code response} the response may contain both successful and failure response items.
         *
         * @param executionId the id of this request, unique for the {@link BulkProcessor} that
         *     created it.
         * @param request the bulk request that will be sent, with documents in binary form.
         * @param contexts application-defined data that was passed in {@link
         *     BulkProcessor#add(BulkOperation, Object)}.
         * @param response the response received from Elasticsearch.
         */
        void afterBulk(
                long executionId,
                BulkRequest request,
                List<Context> contexts,
                BulkResponse response);

        /**
         * Called when a bulk request could not be sent to Elasticsearch.
         *
         * @param executionId the id of this request, unique for the {@link BulkProcessor} that
         *     created it.
         * @param request the bulk request that will be sent, with documents in binary form.
         * @param contexts application-defined data that was passed in {@link
         *     BulkProcessor#add(BulkOperation, Object)}.
         * @param failure the failure that occurred when sending the request to Elasticsearch.
         */
        void afterBulk(
                long executionId, BulkRequest request, List<Context> contexts, Throwable failure);
    }
}
