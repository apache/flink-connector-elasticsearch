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

package org.apache.flink.connector.elasticsearch.bulk;

import org.apache.flink.connector.elasticsearch.DockerImageVersions;
import org.apache.flink.connector.elasticsearch.ElasticsearchServerBaseITCase;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.RequestTest;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Version;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link BulkProcessor}. */
@Testcontainers
public class BulkProcessorTest extends ElasticsearchServerBaseITCase {

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(DockerImageVersions.ELASTICSEARCH_8, LOG)
                    .withPassword(ELASTICSEARCH_PASSWORD);

    private static final Version version =
            DockerImageVersions.getVersion(DockerImageVersions.ELASTICSEARCH_8);

    private static final boolean useTLS = version.major() >= 8;

    private ElasticsearchClient client;

    @Override
    public SSLContext getSSLContext() {
        return useTLS ? ES_CONTAINER.createSslContextFromCa() : null;
    }

    @Override
    public String getElasticsearchHttpHostAddress() {
        String schema = useTLS ? "https" : "http";
        return schema + "://" + ES_CONTAINER.getHttpHostAddress();
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

    private static final BulkOperation operation =
            BulkOperation.of(
                    op ->
                            op.create(
                                    d ->
                                            d.index("foo")
                                                    .id("bar")
                                                    .document(new RequestTest.Product(10))));

    private void printStats(BulkProcessor<?> processor) {
        System.out.printf(
                "BulkProcessor - operations: %d (%d), requests: %d (%d)%n",
                processor.operationsCount(),
                processor.operationContentionsCount(),
                processor.requestCount(),
                processor.requestContentionsCount());
    }

    private void printStats(CountingListener listener) {
        System.out.printf(
                "Listener - operations: %d, requests: %d%n",
                listener.operations.get(), listener.requests.get());
    }

    @BeforeEach
    public void beforeAll() throws IOException {
        client = createElasticsearch8Client();
        client.indices().create(c -> c.index("foo"));
    }

    @Test
    public void basicTestFlush() throws Exception {
        // Prime numbers, so that we have leftovers to flush before shutting down
        multiThreadTest(7, 3, 5, 101, operation);
    }

    private void multiThreadTest(
            int maxOperations,
            int maxRequests,
            int numThreads,
            int numOperations,
            BulkOperation operation)
            throws Exception {
        CountingListener listener = new CountingListener();
        BulkProcessor<Void> bulkProcessor =
                BulkProcessor.of(
                        b ->
                                b.client(client)
                                        .maxOperations(maxOperations)
                                        .maxConcurrentRequests(maxRequests)
                                        .listener(listener));

        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            new Thread(
                            () -> {
                                try {
                                    Thread.sleep((long) (Math.random() * 100));
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                for (int j = 0; j < numOperations; j++) {
                                    bulkProcessor.add(operation);
                                }

                                latch.countDown();
                            })
                    .start();
        }

        latch.await();
        bulkProcessor.flush();
        bulkProcessor.close();
        client.shutdown();

        printStats(bulkProcessor);
        printStats(listener);

        int expectedOperations = numThreads * numOperations;
        assertEquals(expectedOperations, bulkProcessor.operationsCount());
        assertEquals(expectedOperations, listener.operations.get());

        int expectedRequests =
                expectedOperations / maxOperations
                        + ((expectedOperations % maxOperations == 0) ? 0 : 1);

        assertEquals(expectedRequests, bulkProcessor.requestCount());
        assertEquals(expectedRequests, listener.requests.get());
    }

    // -----------------------------------------------------------------------------------------------------------------

    private static class CountingListener implements BulkProcessor.BulkListener<Void> {
        public final AtomicInteger operations = new AtomicInteger();
        public final AtomicInteger requests = new AtomicInteger();

        @Override
        public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {}

        @Override
        public void afterBulk(
                long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
            operations.addAndGet(request.operations().size());
            requests.incrementAndGet();
        }

        @Override
        public void afterBulk(
                long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
            failure.printStackTrace();
            operations.addAndGet(request.operations().size());
            requests.incrementAndGet();
        }
    }
}
