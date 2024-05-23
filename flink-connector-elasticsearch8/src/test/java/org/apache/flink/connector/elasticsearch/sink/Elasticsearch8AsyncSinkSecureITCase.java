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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

import static org.apache.flink.connector.elasticsearch.sink.Elasticsearch8TestUtils.DummyData;
import static org.apache.flink.connector.elasticsearch.sink.Elasticsearch8TestUtils.ELASTICSEARCH_IMAGE;
import static org.apache.flink.connector.elasticsearch.sink.Elasticsearch8TestUtils.assertIdsAreWritten;

/** Integration tests for {@link Elasticsearch8AsyncSink} against a secure Elasticsearch cluster. */
@Testcontainers
class Elasticsearch8AsyncSinkSecureITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch8AsyncSinkSecureITCase.class);
    private static final String ES_CLUSTER_USERNAME = "elastic";
    private static final String ES_CLUSTER_PASSWORD = "s3cret";

    @Container
    private static final ElasticsearchContainer ES_CONTAINER = createSecureElasticsearchContainer();

    private RestClient client;

    @BeforeEach
    void setUp() {
        this.client = getRestClient();
    }

    @AfterEach
    void shutdown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testWriteToSecureElasticsearch8() throws Exception {
        final String index = "test-write-to-secure-elasticsearch8";

        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1)) {

            env.setRestartStrategy(RestartStrategies.noRestart());

            final Elasticsearch8AsyncSink<DummyData> sink =
                    Elasticsearch8AsyncSinkBuilder.<DummyData>builder()
                            .setMaxBatchSize(5)
                            .setHosts(
                                    new HttpHost(
                                            ES_CONTAINER.getHost(),
                                            ES_CONTAINER.getFirstMappedPort(),
                                            "https"))
                            .setElementConverter(
                                    (element, ctx) ->
                                            new IndexOperation.Builder<>()
                                                    .index(index)
                                                    .id(element.getId())
                                                    .document(element)
                                                    .build())
                            .setUsername(ES_CLUSTER_USERNAME)
                            .setPassword(ES_CLUSTER_PASSWORD)
                            .setSslContextSupplier(() -> ES_CONTAINER.createSslContextFromCa())
                            .build();

            env.fromElements("first", "second", "third", "fourth", "fifth")
                    .map(
                            (MapFunction<String, DummyData>)
                                    value -> new DummyData(value + "_v1_index", value))
                    .sinkTo(sink);

            env.execute();
        }

        assertIdsAreWritten(client, index, new String[] {"first_v1_index", "second_v1_index"});
    }

    public static ElasticsearchContainer createSecureElasticsearchContainer() {
        ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withPassword(ES_CLUSTER_PASSWORD) /* set password */
                        .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        // Set log message based wait strategy as the default wait strategy is not aware of TLS
        container
                .withEnv("logger.org.elasticsearch", "INFO")
                .setWaitStrategy(
                        new LogMessageWaitStrategy().withRegEx(".*\"message\":\"started.*"));

        return container;
    }

    private RestClient getRestClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(ES_CLUSTER_USERNAME, ES_CLUSTER_PASSWORD));
        return RestClient.builder(
                        new HttpHost(
                                ES_CONTAINER.getHost(), ES_CONTAINER.getFirstMappedPort(), "https"))
                .setHttpClientConfigCallback(
                        httpClientBuilder ->
                                httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                        .setSSLContext(ES_CONTAINER.createSslContextFromCa()))
                .build();
    }
}
