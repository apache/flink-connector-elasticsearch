/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.BooleanProperty;
import co.elastic.clients.elasticsearch._types.mapping.ByteNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.DateProperty;
import co.elastic.clients.elasticsearch._types.mapping.DenseVectorIndexOptions;
import co.elastic.clients.elasticsearch._types.mapping.DenseVectorProperty;
import co.elastic.clients.elasticsearch._types.mapping.DoubleNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.FloatNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.IntegerNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.ShortNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code VECTOR_SEARCH } ITCase for Elasticsearch 8. */
@Testcontainers
public class Elasticsearch8VectorSearchITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch8VectorSearchITCase.class);

    private static final int PARALLELISM = 2;

    public static final String ELASTICSEARCH_VERSION = "8.19.0";
    public static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                    .withTag(ELASTICSEARCH_VERSION);
    private static final String ES_CLUSTER_USERNAME = "elastic";
    private static final String ES_CLUSTER_PASSWORD = "s3cret";

    @Container
    private static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();

    private static ElasticsearchContainer createElasticsearchContainer() {
        final ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                        .withEnv("logger.org.elasticsearch", "ERROR")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        container.withPassword(ES_CLUSTER_PASSWORD);

        container.setWaitStrategy(
                Wait.defaultWaitStrategy().withStartupTimeout(Duration.ofMinutes(3)));

        return container;
    }

    private String getEsCertFingerprint() throws Exception {
        Preconditions.checkArgument(ES_CONTAINER.caCertAsBytes().isPresent());
        byte[] caCertBytes = ES_CONTAINER.caCertAsBytes().get();
        X509Certificate caCert =
                (X509Certificate)
                        CertificateFactory.getInstance("X.509")
                                .generateCertificate(new ByteArrayInputStream(caCertBytes));
        byte[] fingerprint = MessageDigest.getInstance("SHA-256").digest(caCert.getEncoded());
        return Hex.encodeHexString(fingerprint);
    }

    private ElasticsearchClient getClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(ES_CLUSTER_USERNAME, ES_CLUSTER_PASSWORD));
        RestClient restClient =
                RestClient.builder(
                                new HttpHost(
                                        ES_CONTAINER.getHost(),
                                        ES_CONTAINER.getFirstMappedPort(),
                                        "https"))
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder
                                                .setDefaultCredentialsProvider(credentialsProvider)
                                                .setSSLContext(
                                                        ES_CONTAINER.createSslContextFromCa()))
                        .build();
        RestClientTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    private final List<Row> inputData =
            Arrays.asList(
                    Row.of(1L, "Spark", new Float[] {0.2718f, 0.6527f, 0.7076f}),
                    Row.of(2L, "Flink", new Float[] {-0.2718f, -0.6527f, -0.7076f}));

    private TableEnvironment tEnv;

    @BeforeEach
    void beforeEach() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    public void testSearchFullTypeVectorTable() throws Exception {
        String index = "table_with_all_supported_types";
        createFullTypesIndex(index);

        // Insert data using ES client since elasticsearch-8 connector doesn't support sink
        Map<String, Object> document = new HashMap<>();
        document.put("id", 1L);
        document.put("f1", "ABCDE");
        document.put("f2", true);
        document.put("f3", (byte) 127);
        document.put("f4", (short) 257);
        document.put("f5", 65535);
        document.put("f6", LocalDate.ofEpochDay(12345).toString());
        document.put("f7", "2012-12-12 12:12:12");
        document.put("f8", 11.11f);
        document.put("f9", 12.22d);
        document.put("f10", new float[] {11.11f, 11.12f});
        document.put("f11", new double[] {12.22d, 12.22d});
        document.put("f12", new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE});
        document.put("f13", new long[] {Long.MIN_VALUE, Long.MAX_VALUE});

        IndexResponse response = getClient().index(i -> i.index(index).id("1").document(document));
        LOG.info("Indexed document with result: {}", response.result());

        // Wait for es to refresh index
        getClient().indices().refresh(r -> r.index(index));

        String certFingerprint = getEsCertFingerprint();

        tEnv.executeSql(
                "CREATE TABLE esTable ("
                        + "  id BIGINT,\n"
                        + "  f1 STRING,\n"
                        + "  f2 BOOLEAN,\n"
                        + "  f3 TINYINT,\n"
                        + "  f4 SMALLINT,\n"
                        + "  f5 INTEGER,\n"
                        + "  f6 DATE,\n"
                        + "  f7 TIMESTAMP,\n"
                        + "  f8 FLOAT,\n"
                        + "  f9 DOUBLE,\n"
                        + "  f10 ARRAY<FLOAT>,\n"
                        + "  f11 ARRAY<DOUBLE>,\n"
                        + "  f12 ARRAY<INTEGER>,\n"
                        + "  f13 ARRAY<BIGINT>,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-8")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "https://" + ES_CONTAINER.getHttpHostAddress())
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.USERNAME_OPTION.key(),
                                ES_CLUSTER_USERNAME)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.PASSWORD_OPTION.key(),
                                ES_CLUSTER_PASSWORD)
                        + String.format(
                                "'%s'='%s'\n",
                                Elasticsearch8ConnectorOptions.SSL_CERTIFICATE_FINGERPRINT.key(),
                                certFingerprint)
                        + ")");

        List<String> rows =
                CollectionUtil.iteratorToList(
                                tEnv.executeSql(
                                                "WITH t(id, vector) AS (SELECT * FROM (VALUES (1, CAST(ARRAY[11.11, 1] AS ARRAY<FLOAT>))))\n"
                                                        + "SELECT * FROM t, LATERAL TABLE(VECTOR_SEARCH(TABLE esTable, DESCRIPTOR(f10), t.vector, 3))\n")
                                        .collect())
                        .stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        assertThat(rows)
                .isEqualTo(
                        Collections.singletonList(
                                "+I[1, [11.11, 1.0], 1, ABCDE, true, 127, 257, 65535, 2003-10-20, 2012-12-12T12:12:12, 11.11, 12.22, [11.11, 11.12], [12.22, 12.22], [-2147483648, 2147483647], [-9223372036854775808, 9223372036854775807], 0.8836806]"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cosine", "l2_norm", "dot_product"})
    void testSearchUsingFloatArray(String metric) throws Exception {
        String index = "table_with_multiple_data_with_" + metric.toLowerCase().replace("_", "");
        createSimpleIndex(index, metric);

        // Insert data using ES client since elasticsearch-8 connector doesn't support sink
        // For dot_product, vectors must be normalized (unit vectors)
        indexSimpleDocument(index, "1", 1L, "Batch", new float[] {0.2718f, 0.6527f, 0.7076f});
        indexSimpleDocument(
                index, "2", 2L, "Streaming", new float[] {-0.2718f, -0.6527f, -0.7076f});
        indexSimpleDocument(index, "3", 3L, "Big Data", new float[] {0.7071f, 0.7071f, 0f});

        // Refresh index to make documents searchable
        getClient().indices().refresh(r -> r.index(index));

        String certFingerprint = getEsCertFingerprint();

        tEnv.executeSql(
                "CREATE TABLE es_table("
                        + "  id BIGINT,"
                        + "  label STRING,"
                        + "  vector ARRAY<FLOAT>"
                        + ")\n WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-8")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "https://" + ES_CONTAINER.getHttpHostAddress())
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.USERNAME_OPTION.key(),
                                ES_CLUSTER_USERNAME)
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.PASSWORD_OPTION.key(),
                                ES_CLUSTER_PASSWORD)
                        + String.format(
                                "'%s'='%s'\n",
                                Elasticsearch8ConnectorOptions.SSL_CERTIFICATE_FINGERPRINT.key(),
                                certFingerprint)
                        + ")");

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE src(\n"
                                + "  id BIGINT PRIMARY KEY NOT ENFORCED,\n"
                                + "  content STRING,\n"
                                + "  index ARRAY<FLOAT>\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s'\n"
                                + ");\n",
                        TestValuesTableFactory.registerData(inputData)));
        assertThat(
                        CollectionUtil.iteratorToList(
                                        tEnv.executeSql(
                                                        "SELECT content, label FROM src, LATERAL TABLE(VECTOR_SEARCH(TABLE es_table, DESCRIPTOR(vector), src.index, 2))")
                                                .collect())
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .isEqualTo(
                        Arrays.asList(
                                "+I[Spark, Batch]",
                                "+I[Spark, Big Data]",
                                "+I[Flink, Streaming]",
                                "+I[Flink, Big Data]"));
    }

    private void createFullTypesIndex(String index) throws IOException {
        // In ES 8.x, dense_vector requires index: true and similarity for kNN search
        TypeMapping mapping =
                TypeMapping.of(
                        m ->
                                m.properties(
                                                "id",
                                                Property.of(
                                                        p ->
                                                                p.long_(
                                                                        LongNumberProperty.of(
                                                                                l -> l))))
                                        .properties(
                                                "f1",
                                                Property.of(p -> p.text(TextProperty.of(t -> t))))
                                        .properties(
                                                "f2",
                                                Property.of(
                                                        p ->
                                                                p.boolean_(
                                                                        BooleanProperty.of(
                                                                                b -> b))))
                                        .properties(
                                                "f3",
                                                Property.of(
                                                        p ->
                                                                p.byte_(
                                                                        ByteNumberProperty.of(
                                                                                b -> b))))
                                        .properties(
                                                "f4",
                                                Property.of(
                                                        p ->
                                                                p.short_(
                                                                        ShortNumberProperty.of(
                                                                                s -> s))))
                                        .properties(
                                                "f5",
                                                Property.of(
                                                        p ->
                                                                p.integer(
                                                                        IntegerNumberProperty.of(
                                                                                i -> i))))
                                        .properties(
                                                "f6",
                                                Property.of(p -> p.date(DateProperty.of(d -> d))))
                                        .properties(
                                                "f7",
                                                Property.of(p -> p.text(TextProperty.of(t -> t))))
                                        .properties(
                                                "f8",
                                                Property.of(
                                                        p ->
                                                                p.float_(
                                                                        FloatNumberProperty.of(
                                                                                f -> f))))
                                        .properties(
                                                "f9",
                                                Property.of(
                                                        p ->
                                                                p.double_(
                                                                        DoubleNumberProperty.of(
                                                                                d -> d))))
                                        .properties(
                                                "f10",
                                                Property.of(
                                                        p ->
                                                                p.denseVector(
                                                                        createDenseVectorProperty(
                                                                                2, "cosine"))))
                                        .properties(
                                                "f11",
                                                Property.of(
                                                        p ->
                                                                p.denseVector(
                                                                        createDenseVectorProperty(
                                                                                2, "cosine"))))
                                        .properties(
                                                "f12",
                                                Property.of(
                                                        p ->
                                                                p.denseVector(
                                                                        createDenseVectorProperty(
                                                                                2, "cosine"))))
                                        .properties(
                                                "f13",
                                                Property.of(
                                                        p ->
                                                                p.denseVector(
                                                                        createDenseVectorProperty(
                                                                                2, "cosine")))));

        this.getClient().indices().create(c -> c.index(index).mappings(mapping));
    }

    private void createSimpleIndex(String index, String similarity) throws IOException {
        // In ES 8.x, dense_vector requires index: true and similarity for kNN search
        TypeMapping mapping =
                TypeMapping.of(
                        m ->
                                m.properties(
                                                "id",
                                                Property.of(
                                                        p ->
                                                                p.long_(
                                                                        LongNumberProperty.of(
                                                                                l -> l))))
                                        .properties(
                                                "label",
                                                Property.of(p -> p.text(TextProperty.of(t -> t))))
                                        .properties(
                                                "vector",
                                                Property.of(
                                                        p ->
                                                                p.denseVector(
                                                                        createDenseVectorProperty(
                                                                                3, similarity)))));

        this.getClient().indices().create(c -> c.index(index).mappings(mapping));
    }

    private DenseVectorProperty createDenseVectorProperty(int dims, String similarity) {
        return DenseVectorProperty.of(
                d ->
                        d.dims(dims)
                                .index(true)
                                .similarity(similarity)
                                .indexOptions(
                                        DenseVectorIndexOptions.of(
                                                o -> o.type("hnsw").m(16).efConstruction(100))));
    }

    private void indexSimpleDocument(
            String index, String docId, Long id, String label, float[] vector) throws IOException {
        Map<String, Object> document = new HashMap<>();
        document.put("id", id);
        document.put("label", label);
        document.put("vector", vector);

        IndexResponse response =
                getClient().index(i -> i.index(index).id(docId).document(document));
        LOG.info("Indexed document {} with result: {}", docId, response.result());
    }
}
