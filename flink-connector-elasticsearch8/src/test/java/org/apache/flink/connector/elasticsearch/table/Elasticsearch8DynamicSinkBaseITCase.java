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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.elasticsearch.sink.NetworkConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.transport.TransportUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.connector.elasticsearch.table.TestContext.context;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Elasticsearch8DynamicSinkBaseITCase} is the base class for integration tests.
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
abstract class Elasticsearch8DynamicSinkBaseITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch8DynamicSinkBaseITCase.class);

    public static final String ELASTICSEARCH_VERSION = "8.12.1";
    public static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                    .withTag(ELASTICSEARCH_VERSION);
    protected static final String ES_CLUSTER_USERNAME = "elastic";
    protected static final String ES_CLUSTER_PASSWORD = "s3cret";
    protected static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();
    protected static final ElasticsearchContainer ES_CONTAINER_SECURE =
            createSecureElasticsearchContainer();

    protected static String certificateFingerprint = null;

    // Use singleton test containers which are only started once for several test classes.
    // There is no special support for this use case provided by the Testcontainers
    // extension <code>@Testcontainers</code>.
    static {
        ES_CONTAINER.start();
        ES_CONTAINER_SECURE.start();
    }

    @Parameter public boolean secure;

    protected ElasticsearchAsyncClient client;

    @Parameters(name = "ES secured = {0}")
    public static List<Boolean> secureEnabled() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    public void setUpBase() {
        LOG.info("Setting up elasticsearch client, host: {}, secure: {}", getHost(), secure);
        certificateFingerprint = secure ? getEsCertFingerprint() : null;
        assertThat(secure).isEqualTo(StringUtils.isNotBlank(certificateFingerprint));
        client = secure ? createSecureElasticsearchClient() : createElasticsearchClient();
    }

    @AfterEach
    public void shutdownBase() throws IOException {
        client.shutdown();
    }

    private String getEsCertFingerprint() {
        if (!ES_CONTAINER_SECURE.caCertAsBytes().isPresent()) {
            LOG.error("Cannot get the CA cert from the docker container.");
            return null;
        }

        byte[] caCertBytes = ES_CONTAINER_SECURE.caCertAsBytes().get();

        CertificateFactory cf;
        byte[] fingerprintBytes = new byte[0];
        try {
            cf = CertificateFactory.getInstance("X.509");
            X509Certificate caCert =
                    (X509Certificate)
                            cf.generateCertificate(new java.io.ByteArrayInputStream(caCertBytes));

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            fingerprintBytes = md.digest(caCert.getEncoded());
        } catch (CertificateException | NoSuchAlgorithmException e) {
            LOG.error("Failed to compute certificate fingerprint: ", e);
        }

        return Hex.encodeHexString(fingerprintBytes);
    }

    TestContext getPrefilledTestContext(String index) {
        TestContext testContext =
                context()
                        .withOption(Elasticsearch8ConnectorOptions.INDEX_OPTION.key(), index)
                        .withOption(
                                Elasticsearch8ConnectorOptions.HOSTS_OPTION.key(),
                                secure
                                        ? "https://" + ES_CONTAINER_SECURE.getHttpHostAddress()
                                        : "http://" + ES_CONTAINER.getHttpHostAddress());
        if (secure) {
            testContext
                    .withOption(
                            Elasticsearch8ConnectorOptions.USERNAME_OPTION.key(),
                            ES_CLUSTER_USERNAME)
                    .withOption(
                            Elasticsearch8ConnectorOptions.PASSWORD_OPTION.key(),
                            ES_CLUSTER_PASSWORD)
                    .withOption(
                            Elasticsearch8ConnectorOptions.SSL_CERTIFICATE_FINGERPRINT.key(),
                            certificateFingerprint);
        }
        return testContext;
    }

    @SuppressWarnings({"unchecked"})
    Map<String, Object> makeGetRequest(String index, String id)
            throws ExecutionException, InterruptedException {
        return (Map<String, Object>)
                client.get(new GetRequest.Builder().index(index).id(id).build(), Map.class)
                        .get()
                        .source();
    }

    @SuppressWarnings({"unchecked"})
    List<Map<String, Object>> makeSearchRequest(String index)
            throws ExecutionException, InterruptedException {
        return client.search(new SearchRequest.Builder().index(index).build(), Map.class).get()
                .hits().hits().stream()
                .map(hit -> (Map<String, Object>) hit.source())
                .collect(Collectors.toList());
    }

    String getConnectorSql(String index) {
        if (secure) {
            return String.format("'%s'='%s',\n", "connector", "elasticsearch-8")
                    + String.format(
                            "'%s'='%s',\n",
                            Elasticsearch8ConnectorOptions.INDEX_OPTION.key(), index)
                    + String.format(
                            "'%s'='%s',\n",
                            Elasticsearch8ConnectorOptions.HOSTS_OPTION.key(),
                            "https://" + ES_CONTAINER_SECURE.getHttpHostAddress())
                    + String.format(
                            "'%s'='%s',\n",
                            Elasticsearch8ConnectorOptions.USERNAME_OPTION.key(),
                            ES_CLUSTER_USERNAME)
                    + String.format(
                            "'%s'='%s',\n",
                            Elasticsearch8ConnectorOptions.PASSWORD_OPTION.key(),
                            ES_CLUSTER_PASSWORD)
                    + String.format(
                            "'%s'='%s'\n",
                            Elasticsearch8ConnectorOptions.SSL_CERTIFICATE_FINGERPRINT.key(),
                            certificateFingerprint);
        } else {
            return String.format("'%s'='%s',\n", "connector", "elasticsearch-8")
                    + String.format(
                            "'%s'='%s',\n",
                            Elasticsearch8ConnectorOptions.INDEX_OPTION.key(), index)
                    + String.format(
                            "'%s'='%s'\n",
                            Elasticsearch8ConnectorOptions.HOSTS_OPTION.key(),
                            "http://" + ES_CONTAINER.getHttpHostAddress());
        }
    }

    private static ElasticsearchContainer createElasticsearchContainer() {
        final ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withEnv("xpack.security.enabled", "false")
                        .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                        .withEnv("logger.org.elasticsearch", "ERROR")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        container.setWaitStrategy(
                Wait.defaultWaitStrategy().withStartupTimeout(Duration.ofMinutes(5)));

        return container;
    }

    private static ElasticsearchContainer createSecureElasticsearchContainer() {
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

    private ElasticsearchAsyncClient createElasticsearchClient() {
        return new NetworkConfig(
                        Collections.singletonList(getHost()),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null)
                .createEsClient();
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

    private ElasticsearchAsyncClient createSecureElasticsearchClient() {
        return new NetworkConfig(
                        Collections.singletonList(getHost()),
                        ES_CLUSTER_USERNAME,
                        ES_CLUSTER_PASSWORD,
                        null,
                        null,
                        null,
                        null,
                        null,
                        () -> TransportUtils.sslContextFromCaFingerprint(certificateFingerprint),
                        null)
                .createEsClient();
    }

    /** A mock {@link DynamicTableSink.Context} for Elasticsearch tests. */
    static class MockContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public TypeInformation<?> createTypeInformation(LogicalType consumedLogicalType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }

        public Optional<int[][]> getTargetColumns() {
            return Optional.empty();
        }
    }
}
