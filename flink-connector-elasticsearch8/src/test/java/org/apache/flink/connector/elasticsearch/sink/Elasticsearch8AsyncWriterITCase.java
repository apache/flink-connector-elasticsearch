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

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.metrics.Gauge;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link Elasticsearch8AsyncWriter}. */
public class Elasticsearch8AsyncWriterITCase extends ElasticsearchSinkBaseITCase {
    private TestSinkInitContext context;

    private final Lock lock = new ReentrantLock();

    private final Condition completed = lock.newCondition();
    private final AtomicBoolean completedExceptionally = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        this.context = new TestSinkInitContext();
    }

    @TestTemplate
    @Timeout(5)
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
    @Timeout(5)
    public void testBulkOnBufferTimeFlush() throws Exception {
        String index = "test-bulk-on-time-in-buffer";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.flush(true);

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
    @Timeout(5)
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
    @Timeout(5)
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
    @Timeout(5)
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
    @Timeout(5)
    public void testHandlePartiallyFailedBulk() throws Exception {
        String index = "test-partially-failed-bulk";
        int maxBatchSize = 3;

        // First create a document to enable version conflict
        try (final Elasticsearch8AsyncWriter<DummyData> setupWriter = createWriter(index, 1)) {
            setupWriter.write(new DummyData("test-3", "test-3"), null);
            await();
        }

        // Create converter that triggers 409 version conflict for test-3
        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> conflictConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) -> {
                            if (element.getId().equals("test-3")) {
                                // Use wrong version to trigger 409 conflict (retryable)
                                return new IndexOperation.Builder<>()
                                        .id(element.getId())
                                        .index(index)
                                        .document(element)
                                        .ifSeqNo(999L) // Wrong sequence number
                                        .ifPrimaryTerm(1L)
                                        .build();
                            } else {
                                return new IndexOperation.Builder<>()
                                        .id(element.getId())
                                        .index(index)
                                        .document(element)
                                        .build();
                            }
                        });

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(maxBatchSize, conflictConverter)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "version-conflict"), null);
        }

        await();

        // 409 is retryable, so test-3 should have not completed the rest handler exceptionally
        assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(1);
        assertThat(completedExceptionally.get()).isFalse();
        assertIdsAreWritten(index, new String[] {"test-1", "test-2"});
    }

    @TestTemplate
    @Timeout(5)
    public void testFailFastUponPartiallyFailedBulk() throws Exception {
        String index = "test-fail-fast-partially-failed-bulk";
        int maxBatchSize = 2;

        // This simulates a scenario where some operations fail with non-retryable errors.
        // test-1 gets docAsUpsert=false on non-existing doc (404 error).
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

        // Verify that non-retryable error (404) increments error counter and fails fast
        assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(1);
        assertThat(completedExceptionally.get()).isTrue();
        assertIdsAreWritten(index, new String[] {"test-2"});
        assertIdsAreNotWritten(index, new String[] {"test-1"});
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(String index, int maxBatchSize)
            throws IOException {
        return createWriter(
                maxBatchSize,
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        getElementConverterForDummyData(index)));
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
            Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter)
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
                        createNetworkConfig()) {
                    @Override
                    public StatefulSinkWriter createWriter(WriterInitContext context) {
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
                                Collections.emptyList()) {
                            @Override
                            protected void submitRequestEntries(
                                    List<Operation> requestEntries,
                                    ResultHandler<Operation> resultHandler) {
                                ResultHandler<Operation> wrappedHandler =
                                        new ResultHandler<Operation>() {
                                            @Override
                                            public void complete() {
                                                resultHandler.complete();
                                                signal();
                                            }

                                            @Override
                                            public void completeExceptionally(Exception e) {
                                                resultHandler.completeExceptionally(e);
                                                completedExceptionally.set(true);
                                                signal();
                                            }

                                            @Override
                                            public void retryForEntries(List<Operation> list) {
                                                resultHandler.retryForEntries(list);
                                                signal();
                                            }
                                        };
                                super.submitRequestEntries(requestEntries, wrappedHandler);
                            }
                        };
                    }
                };

        return (Elasticsearch8AsyncWriter<DummyData>) sink.createWriter(context);
    }

    private void signal() {
        lock.lock();
        try {
            completed.signal();
        } finally {
            lock.unlock();
        }
    }

    private void await() throws InterruptedException {
        lock.lock();
        try {
            completed.await();
        } finally {
            lock.unlock();
        }
    }
}
