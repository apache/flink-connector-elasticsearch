/*
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
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.json.JsonData;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Integration tests for {@link Elasticsearch8AsyncWriter}. */
public class Elasticsearch8AsyncWriterITCase extends ElasticsearchSinkBaseITCase {
    private TestSinkInitContext context;

    private final Lock lock = new ReentrantLock();
    private final Condition completed = lock.newCondition();
    private final AtomicInteger pendingCallbacks = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        this.context = new TestSinkInitContext();
    }

    @TestTemplate
    @Timeout(20)
    public void testBulkOnFlush() throws IOException, InterruptedException {
        String index = "test-bulk-on-flush";
        int maxBatchSize = 2;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);

            writer.flush(false);
            assertIdsAreWritten(index, new String[] {"test-1", "test-2"});

            writer.write(new DummyData("3", "test-3"), null);
            writer.flush(true);
            assertIdsAreWritten(index, new String[] {"test-3"});
        }
    }

    @TestTemplate
    @Timeout(20)
    public void testBulkOnBufferTimeFlush() throws Exception {
        String index = "test-bulk-on-time-in-buffer";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.flush(true);
            await();
            assertIdsAreWritten(index, new String[] {"test-1"});

            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            assertIdsAreNotWritten(index, new String[] {"test-2", "test-3"});
            context.getTestProcessingTimeService().advance(6000L);

            await();
        }

        assertIdsAreWritten(index, new String[] {"test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testBytesSentMetric() throws Exception {
        String index = "test-bytes-sent-metrics";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(index, maxBatchSize)) {
            assertThat(context.getNumBytesOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumBytesOutCounter().getCount()).isGreaterThan(0);
        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testRecordsSentMetric() throws Exception {
        String index = "test-records-sent-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(index, maxBatchSize)) {
            assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(3);
        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testSendTimeMetric() throws Exception {
        String index = "test-send-time-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(index, maxBatchSize)) {
            final Optional<Gauge<Long>> currentSendTime = context.getCurrentSendTimeGauge();

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }

        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testHandlePartiallyFailedBulk() throws Exception {
        String index = "test-partially-failed-bulk";
        int maxBatchSize = 2;

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) ->
                                new UpdateOperation.Builder<>()
                                        .id(element.getId())
                                        .index(index)
                                        .action(
                                                ac ->
                                                        ac.doc(element)
                                                                .docAsUpsert(
                                                                        element.getId()
                                                                                .equals("test-2")))
                                        .build());

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(maxBatchSize, elementConverter)) {
            writer.write(new DummyData("test-1", "test-1-updated"), null);
            writer.write(new DummyData("test-2", "test-2-updated"), null);
        }

        await();

        assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(1);
        assertIdsAreWritten(index, new String[] {"test-2"});
        assertIdsAreNotWritten(index, new String[] {"test-1"});
    }

    @TestTemplate
    @Timeout(20)
    public void testEmergencyModeDropsRecords() throws Exception {
        String index = "test-emergency-mode-drop";
        int maxBatchSize = 5;
        int maxRetries = 2;

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> poisonPillConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) -> {
                            if (element.getId().equals("poison-pill")) {
                                Map<String, Object> badDoc = Collections.singletonMap("data", Collections.singletonMap("nested", "value"));
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(badDoc))
                                        .build();
                            } else {
                                Map<String, Object> goodDoc = Collections.singletonMap("data", "valid-payload-string");
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(goodDoc))
                                        .build();
                            }
                        });

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(maxBatchSize, poisonPillConverter, true, maxRetries)) {
            writer.write(new DummyData("valid-1", "valid-1"), null);
            writer.flush(true);
            assertIdsAreWritten(index, new String[] {"valid-1"});
            writer.write(new DummyData("valid-2", "valid-2"), null);
            writer.write(new DummyData("poison-pill", "poison"), null);
            writer.write(new DummyData("valid-3", "valid-3"), null);
            writer.flush(true);

            assertIdsAreWritten(index, new String[] {"valid-1", "valid-2", "valid-3"});
            assertIdsAreNotWritten(index, new String[] {"poison-pill"}); // PROOF it was dropped
            waitForMetric(() -> context.metricGroup().getNumRecordsOutErrorsCounter().getCount() > 0);
            assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isGreaterThan(0);
        }
    }

    @TestTemplate
    @Timeout(20)
    public void testNonEmergencyModeFailsJob() throws Exception {
        String index = "test-non-emergency-fail";
        int maxBatchSize = 5;
        int maxRetries = 2;

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> poisonPillConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) -> {
                            if (element.getId().equals("poison-pill")) {
                                Map<String, Object> badDoc = Collections.singletonMap("data", Collections.singletonMap("nested", "value"));
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(badDoc))
                                        .build();
                            } else {
                                Map<String, Object> goodDoc = Collections.singletonMap("data", "valid-payload-string");
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(goodDoc))
                                        .build();
                            }
                        });

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                     createWriter(maxBatchSize, poisonPillConverter, false, maxRetries)) {
            writer.write(new DummyData("valid-1", "valid-1"), null);
            writer.flush(true);
            writer.write(new DummyData("poison-pill", "poison"), null);
            assertThrows(FlinkRuntimeException.class, () -> writer.flush(true));
        }
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(String index, int maxBatchSize)
            throws IOException {
        return createWriter(
                maxBatchSize,
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        getElementConverterForDummyData(index)),
                false,
                5
        );
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(
            int maxBatchSize,
            Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter)
            throws IOException {
        return createWriter(maxBatchSize, elementConverter, false, 5);
    }

    private NetworkConfig createNetworkConfig() {
        final List<HttpHost> esHost = Collections.singletonList(getHost());
        return secure
                ? new NetworkConfig(
                esHost,
                ES_CLUSTER_USERNAME,
                ES_CLUSTER_PASSWORD,
                null,
                () -> ES_CONTAINER_SECURE.createSslContextFromCa(),
                null)
                : new NetworkConfig(esHost, null, null, null, null, null);
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(
            int maxBatchSize,
            Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter,
            boolean emergencyMode,
            int maxRetries)
            throws IOException {

        Elasticsearch8AsyncSink<DummyData> sink =
                new Elasticsearch8AsyncSink<DummyData>(
                        elementConverter,
                        maxBatchSize,
                        50,
                        10_000,
                        5 * 1024 * 1024,
                        5000,
                        1024 * 1024,
                        createNetworkConfig(),
                        emergencyMode,
                        maxRetries
                ) {
                    @Override
                    public StatefulSinkWriter<DummyData, BufferedRequestState<RetryableOperation>> createWriter(WriterInitContext context) {
                        return new Elasticsearch8AsyncWriter<DummyData>(
                                getElementConverter(),
                                context,
                                maxBatchSize,
                                getMaxInFlightRequests(),
                                getMaxBufferedRequests(),
                                getMaxBatchSizeInBytes(),
                                getMaxTimeInBufferMS(),
                                getMaxRecordSizeInBytes(),
                                networkConfig,
                                Collections.emptyList(),
                                emergencyMode,
                                maxRetries
                        ) {
                            @Override
                            protected void submitRequestEntries(
                                    List<RetryableOperation> requestEntries,
                                    ResultHandler<RetryableOperation> resultHandler) {

                                ResultHandler<RetryableOperation> wrappedHandler =
                                        new ResultHandler<RetryableOperation>() {
                                            @Override
                                            public void complete() {
                                                resultHandler.complete();
                                                signal();
                                            }

                                            @Override
                                            public void completeExceptionally(Exception e) {
                                                resultHandler.completeExceptionally(e);
                                                signal();
                                            }

                                            @Override
                                            public void retryForEntries(List<RetryableOperation> list) {
                                                resultHandler.retryForEntries(list);
                                                signal();
                                            }
                                        };
                                try {
                                    super.submitRequestEntries(requestEntries, wrappedHandler);
                                } catch (Exception e) {
                                    wrappedHandler.completeExceptionally(e);
                                }
                            }
                        };
                    }
                };

        return (Elasticsearch8AsyncWriter<DummyData>) sink.createWriter(context);
    }

    private void signal() {
        lock.lock();
        try {
            pendingCallbacks.incrementAndGet();
            completed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void await() throws InterruptedException {
        lock.lock();
        try {
            while (pendingCallbacks.get() == 0) {
                if (!completed.await(10, TimeUnit.SECONDS)) {
                    throw new java.util.concurrent.TimeoutException("Timed out waiting for AsyncSinkWriter callback");
                }
            }
            pendingCallbacks.decrementAndGet();
        } catch (java.util.concurrent.TimeoutException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void waitForMetric(Supplier<Boolean> condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        lock.lock();
        try {
            while (!condition.get()) {
                if (System.nanoTime() > deadline) {
                    throw new RuntimeException("Timeout waiting for metric condition");
                }
                completed.await(100, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }
}
