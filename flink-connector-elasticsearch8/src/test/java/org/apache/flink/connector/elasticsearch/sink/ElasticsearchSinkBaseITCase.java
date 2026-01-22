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

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ElasticsearchSinkBaseITCase} is the base class for integration tests.
 *
 * <p>It is extended with the {@link ParameterizedTestExtension} for parameterized testing against
 * secure and non-secure Elasticsearch clusters. Tests must be annotated by {@link TestTemplate} in
 * order to be parameterized.
 *
 * <p>The cluster is running via test containers. In order to reuse the singleton containers by all
 * inheriting test classes, we manage their lifecycle. The two containers are started only once when
 * this class is loaded. At the end of the test suite the Ryuk container that is started by
 * Testcontainers core will take care of stopping the singleton container.
 */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class ElasticsearchSinkBaseITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkBaseITCase.class);

    public static final String ELASTICSEARCH_VERSION = "8.12.1";
    public static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                    .withTag(ELASTICSEARCH_VERSION);
    protected static final String ES_CLUSTER_USERNAME = "elastic";
    protected static final String ES_CLUSTER_PASSWORD = "s3cret";

    protected static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();
    protected static final ElasticsearchContainer ES_CONTAINER_SECURE =
            createSecureElasticsearchContainer();

    // Use singleton test containers which are only started once for several test classes.
    // There is no special support for this use case provided by the Testcontainers
    // extension <code>@Testcontainers</code>.
    static {
        ES_CONTAINER.start();
        ES_CONTAINER_SECURE.start();
    }

    @Parameter public boolean secure;

    protected RestClient client;

    @Parameters(name = "ES secured = {0}")
    public static List<Boolean> secureEnabled() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    public void setUpBase() throws InterruptedException{
        LOG.info("Setting up elasticsearch client, host: {}, secure: {}", getHost(), secure);
        client = secure ? createSecureElasticsearchClient() : createElasticsearchClient();
        waitForElasticsearch();
    }

    @AfterEach
    public void shutdownBase() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    /** Get the element converter for testing data type {@link DummyData}. */
    protected ElementConverter<DummyData, BulkOperationVariant> getElementConverterForDummyData(
            String index) {
        return (element, ctx) ->
                new IndexOperation.Builder<DummyData>()
                        .id(element.getId())
                        .document(element)
                        .index(index)
                        .build();
    }

    /** Create an Elasticsearch8AsyncSink for DummyData type against the ES test container. */
    protected Elasticsearch8AsyncSink<DummyData> getSinkForDummyData(String index) {
        final Elasticsearch8AsyncSinkBuilder<DummyData> builder =
                Elasticsearch8AsyncSinkBuilder.<DummyData>builder()
                        .setHosts(getHost())
                        .setMaxBatchSize(5)
                        .setElementConverter(getElementConverterForDummyData(index));

        if (secure) {
            builder.setUsername(ES_CLUSTER_USERNAME)
                    .setPassword(ES_CLUSTER_PASSWORD)
                    .setSslContextSupplier(() -> ES_CONTAINER_SECURE.createSslContextFromCa());
        }

        return builder.build();
    }

    /** Get Elasticsearch host depending on the parameter secure. */
    protected HttpHost getHost() {
        return secure
                ? new HttpHost(
                        ES_CONTAINER_SECURE.getHost(),
                        ES_CONTAINER_SECURE.getFirstMappedPort(),
                        "https")
                : new HttpHost(ES_CONTAINER.getHost(), ES_CONTAINER.getFirstMappedPort());
    }

    protected void assertIdsAreWritten(String index, String[] ids) throws IOException {
        final String responseEntity = queryElasticsearchIndex(index);
        for (String id : ids) {
            LOG.info("Checking document id {}", id);
            assertThat(responseEntity).contains(id);
        }
    }

    protected void assertIdsAreNotWritten(String index, String[] ids) throws IOException {
        final String responseEntity = queryElasticsearchIndex(index);
        for (String id : ids) {
            assertThat(responseEntity).doesNotContain(id);
        }
    }

    /** DummyData is a POJO to helping during integration tests. */
    public static class DummyData {
        private final String id;

        private final String name;

        public DummyData(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    private static ElasticsearchContainer createElasticsearchContainer() {
        final ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withEnv("xpack.security.enabled", "false")
                        .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                        .withEnv("logger.org.elasticsearch", "ERROR")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        container.setWaitStrategy(
                new org.testcontainers.containers.wait.strategy.HttpWaitStrategy()
                        .forPort(9200)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        return container;
    }

    /**
     * Blocks until Elasticsearch is reachable and healthy.
     * This prevents tests from failing due to startup race conditions.
     */
    protected void waitForElasticsearch() throws InterruptedException {
        LOG.info("Waiting for Elasticsearch to become ready...");
        int maxAttempts = 60;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                Response response = client.performRequest(new Request("GET", "/"));
                if (response.getStatusLine().getStatusCode() == 200) {
                    LOG.info("Elasticsearch is ready!");
                    return;
                }
            } catch (IOException e) {
                if (i > 55) {
                    LOG.info("Still waiting for Elasticsearch (Attempt {}/{}): {}", i + 1, maxAttempts, e.getMessage());
                }
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("Elasticsearch did not become ready in time.");
    }

    private static ElasticsearchContainer createSecureElasticsearchContainer() {
        ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withPassword(ES_CLUSTER_PASSWORD) /* set password */
                        .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        container
                .withEnv("logger.org.elasticsearch", "INFO")
                .setWaitStrategy(
                        new LogMessageWaitStrategy().withRegEx(".*\"message\":\"started.*"));

        return container;
    }

    private RestClient createElasticsearchClient() {
        return RestClient.builder(getHost())
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder)
                .build();
    }

    private RestClient createSecureElasticsearchClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(ES_CLUSTER_USERNAME, ES_CLUSTER_PASSWORD));
        return RestClient.builder(getHost())
                .setHttpClientConfigCallback(
                        httpClientBuilder ->
                                httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                        .setSSLContext(
                                                ES_CONTAINER_SECURE.createSslContextFromCa()))
                .build();
    }

    private String queryElasticsearchIndex(String index) throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());
        LOG.debug("Got response: {}", responseEntity);

        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

        return responseEntity;
    }
}
