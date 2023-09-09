/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.DockerImageVersions;
import org.apache.flink.connector.elasticsearch.ElasticsearchServerBaseITCase;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.RequestTest;
import org.apache.flink.connector.elasticsearch.bulk.BulkProcessor;
import org.apache.flink.connector.elasticsearch.bulk.RequestIndexer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.ThrowingRunnable;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Version;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.elasticsearch.sink.TestClientBase.buildMessage;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Elasticsearch8Writer}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class Elasticsearch8WriterITCase extends ElasticsearchServerBaseITCase {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8WriterITCase.class);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(DockerImageVersions.ELASTICSEARCH_8, LOG)
                    .withPassword(ELASTICSEARCH_PASSWORD);

    private static final Version version =
            DockerImageVersions.getVersion(DockerImageVersions.ELASTICSEARCH_8);

    private static final boolean useTLS = version.major() >= 8;

    @Override
    public SSLContext getSSLContext() {
        return useTLS ? ES_CONTAINER.createSslContextFromCa() : null;
    }

    @Override
    public String getElasticsearchHttpHostAddress() {
        String schema = useTLS ? "https" : "http";
        return schema + "://" + ES_CONTAINER.getHttpHostAddress();
    }

    private ElasticsearchClient client;
    private TestClientBase context;
    private MetricListener metricListener;

    @BeforeEach
    void setUp() {
        metricListener = new MetricListener();
        client = createElasticsearch8Client();
        context = new TestClient(client);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public ElasticsearchClient createElasticsearch8Client() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD));
        HttpHost host = HttpHost.create(getElasticsearchHttpHostAddress());
        RestClient httpClient =
                RestClient.builder(host)
                        .setHttpClientConfigCallback(
                                hc ->
                                        hc.setDefaultCredentialsProvider(credentialsProvider)
                                                .setSSLContext(getSSLContext()))
                        .build();
        ElasticsearchTransport transport =
                new RestClientTransport(httpClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    @Test
    void testWriteOnBulkFlush() throws Exception {
        final String index = "test-bulk-flush-without-checkpoint";
        final int flushAfterNActions = 5;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(
                        flushAfterNActions, 10000, 10000, FlushBackoffType.NONE, 3, 10000);

        try (final Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);

            // Ignore flush on checkpoint
            writer.flush(false);

            context.assertThatIdsAreNotWritten(index, 1, 2, 3, 4);

            // Trigger flush
            writer.write(Tuple2.of(5, buildMessage(5)), null);
            Thread.sleep(10);
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);

            writer.write(Tuple2.of(6, buildMessage(6)), null);
            context.assertThatIdsAreNotWritten(index, 6);

            // Force flush
            writer.blockingFlushAllActions();
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    void testWriteOnBulkIntervalFlush() throws Exception {
        final String index = "test-bulk-flush-with-interval";

        // Configure bulk processor to flush every 1s;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(-1, -1, 1000, FlushBackoffType.NONE, 0, 0);

        try (final Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);
            writer.blockingFlushAllActions();
        }

        context.assertThatIdsAreWritten(index, 1, 2, 3, 4);
    }

    @Test
    void testWriteOnCheckpoint() throws Exception {
        final String index = "test-bulk-flush-with-checkpoint";
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(-1, -1, -1, FlushBackoffType.NONE, 0, 0);

        // Enable flush on checkpoint
        try (final Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> writer =
                createWriter(index, true, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);

            context.assertThatIdsAreNotWritten(index, 1, 2, 3);

            // Trigger flush
            writer.flush(false);

            context.assertThatIdsAreWritten(index, 1, 2, 3);
        }
    }

    @Test
    void testIncrementByteOutMetric() throws Exception {
        final String index = "test-inc-byte-out";
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> writer =
                createWriter(index, false, bulkProcessorConfig, metricGroup)) {
            final Counter numBytesOut = operatorIOMetricGroup.getNumBytesOutCounter();
            assertThat(numBytesOut.getCount()).isZero();
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();
            long first = numBytesOut.getCount();
            assertThat(first).isGreaterThan(0);

            writer.write(Tuple2.of(3, buildMessage(4)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);

            writer.blockingFlushAllActions();
            assertThat(numBytesOut.getCount()).isGreaterThan(first);
        }
    }

    @Test
    void testIncrementRecordsSendMetric() throws Exception {
        final String index = "test-inc-records-send";
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            final Optional<Counter> recordsSend =
                    metricListener.getCounter(MetricNames.NUM_RECORDS_SEND);
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            // Update existing index
            writer.write(Tuple2.of(1, buildMessage(2, "u" + "test_" + 2)), null);
            // Delete index
            writer.write(Tuple2.of(1, buildMessage(3, "d" + "test_" + 2)), null);

            writer.blockingFlushAllActions();

            assertThat(recordsSend).isPresent();
            assertThat(recordsSend.get().getCount()).isEqualTo(3L);
        }
    }

    @Test
    void testCurrentSendTime() throws Exception {
        final String index = "test-current-send-time";
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }
    }

    private Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> createWriter(
            String index, boolean flushOnCheckpoint, BulkProcessorConfig bulkProcessorConfig)
            throws Exception {
        return createWriter(
                index,
                flushOnCheckpoint,
                bulkProcessorConfig,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()));
    }

    private Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>> createWriter(
            String index,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            SinkWriterMetricGroup metricGroup)
            throws Exception {
        return new Elasticsearch8Writer<Tuple2<Integer, RequestTest.Product>>(
                Collections.singletonList(HttpHost.create(getElasticsearchHttpHostAddress())),
                getSSLContext(),
                new TestingEmitter(index, context.getDataFieldName()),
                flushOnCheckpoint,
                bulkProcessorConfig,
                getBulkProcessorBuilderFactory(),
                new NetworkClientConfig("elastic", ELASTICSEARCH_PASSWORD, null, null, null, null),
                metricGroup,
                new TestMailbox());
    }

    protected BulkProcessorFactory<Tuple2<Integer, RequestTest.Product>>
            getBulkProcessorBuilderFactory() {
        return new BulkProcessorFactory<Tuple2<Integer, RequestTest.Product>>() {
            @Override
            public BulkProcessor.Builder<Tuple2<Integer, RequestTest.Product>> apply(
                    ElasticsearchClient client,
                    BulkProcessorConfig bulkProcessorConfig,
                    BulkProcessor.BulkListener<Tuple2<Integer, RequestTest.Product>> listener) {
                BulkProcessor.Builder<Tuple2<Integer, RequestTest.Product>> builder =
                        BulkProcessor.ofBuilder(
                                b ->
                                        b.client(client)
                                                .listener(listener)
                                                .maxOperations(
                                                        bulkProcessorConfig
                                                                .getBulkFlushMaxActions())
                                                .maxSize(bulkProcessorConfig.getBulkFlushMaxMb())
                                                .flushInterval(
                                                        bulkProcessorConfig.getBulkFlushInterval(),
                                                        TimeUnit.MILLISECONDS));
                return builder;
            }
        };
    }

    private static class TestingEmitter
            implements Elasticsearch8Emitter<Tuple2<Integer, RequestTest.Product>> {

        private final String dataFieldName;
        private final String index;

        TestingEmitter(String index, String dataFieldName) {
            this.index = index;
            this.dataFieldName = dataFieldName;
        }

        @Override
        public void emit(
                Tuple2<Integer, RequestTest.Product> element,
                SinkWriter.Context context,
                RequestIndexer indexer) {

            final char action = element.f1.name.charAt(0);
            final String id = element.f0.toString();
            switch (action) {
                case 'd':
                    {
                        DeleteOperation deleteOperation =
                                DeleteOperation.of(b -> b.index(String.valueOf(index)).id(id));
                        BulkOperation bulkOperation =
                                BulkOperation.of(f -> f.delete(deleteOperation));
                        indexer.add(bulkOperation);
                        break;
                    }
                case 'u':
                    {
                        UpdateOperation<Object, Object> updateOperation =
                                UpdateOperation.of(
                                        b ->
                                                b.index(String.valueOf(index))
                                                        .id(id)
                                                        .action(
                                                                _3 ->
                                                                        _3.docAsUpsert(true)
                                                                                .doc(element.f1)));
                        BulkOperation bulkOperation =
                                BulkOperation.of(f -> f.update(updateOperation));
                        indexer.add(bulkOperation);
                        break;
                    }
                default:
                    {
                        CreateOperation<Object> createOperation =
                                CreateOperation.of(
                                        b ->
                                                b.index(String.valueOf(index))
                                                        .id(id)
                                                        .document(element.f1));
                        BulkOperation bulkOperation =
                                BulkOperation.of(f -> f.create(createOperation));
                        indexer.add(bulkOperation);
                    }
            }
        }
    }

    private static class TestClient extends TestClientBase {

        TestClient(ElasticsearchClient client) {
            super(client);
        }

        @Override
        GetResponse getResponse(String index, int id) throws ElasticsearchException, IOException {
            GetRequest getRequest = GetRequest.of(f -> f.index(index).id(Integer.toString(id)));
            int retry = 3;
            while (retry > 0) {
                try {
                    retry--;
                    return client.get(
                            getRequest, new TypeReference<RequestTest.Product>() {}.getType());
                } catch (ElasticsearchException e) {
                    try {

                        Thread.sleep(3);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    if (retry == 1) {
                        LOG.warn("Get response failed", e);
                    }
                } catch (ResponseException re) {
                    // Note: Catch this exception until the data is written successfully
                    if (re.getMessage().contains("no_shard_available_action_exception")) {
                        retry++;
                    }
                }
            }
            return GetResponse.of(b -> b.id(String.valueOf(id)).index(index).found(false));
        }
    }

    private static class TestMailbox implements MailboxExecutor {

        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            try {
                command.run();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected error", e);
            }
        }

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {
            Thread.sleep(100);
        }

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }
    }
}
