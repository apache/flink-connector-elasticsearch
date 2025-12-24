package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.test.DockerImageVersions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;

/** {@code VECTOR_SEARCH } ITCase for Elasticsearch. */
@Testcontainers
public class Elasticsearch7VectorSearchITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch7VectorSearchITCase.class);

    private static final int PARALLELISM = 2;

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_7, LOG);

    String getElasticsearchHttpHostAddress() {
        return ES_CONTAINER.getHttpHostAddress();
    }

    private RestHighLevelClient getClient() {
        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(getElasticsearchHttpHostAddress())));
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
                    Row.of(1L, "Spark", new Float[] {5f, 12f, 13f}),
                    Row.of(2L, "Flink", new Float[] {-5f, -12f, -13f}));

    private TableEnvironment tEnv;

    @BeforeEach
    void beforeEach() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    public void testSearchFullTypeVectorTable() throws Exception {
        String index = "table_with_all_supported_types";
        createFullTypesIndex(index);
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
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-7")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s'\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                ES_CONTAINER.getHttpHostAddress())
                        + ")");

        tEnv.fromValues(
                        row(
                                1,
                                "ABCDE",
                                true,
                                (byte) 127,
                                (short) 257,
                                65535,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12"),
                                11.11f,
                                12.22d,
                                new Float[] {11.11f, 11.12f},
                                new Double[] {12.22d, 12.22d},
                                new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE},
                                new long[] {Long.MIN_VALUE, Long.MAX_VALUE}))
                .executeInsert("esTable")
                .await();

        // Wait for es construct index.
        Thread.sleep(2000);

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
                                "+I[1, [11.11, 1.0], 1, ABCDE, true, 127, 257, 65535, 2003-10-20, 2012-12-12T12:12:12, 11.11, 12.22, [11.11, 11.12], [12.22, 12.22], [-2147483648, 2147483647], [-9223372036854775808, 9223372036854775807], 1.767361044883728]"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cosineSimilarity", "l1norm", "l2norm", "hamming", "dotProduct"})
    void testSearchUsingFloatArray(String metric) throws Exception {
        String index = "table_with_multiple_data_with_" + metric.toLowerCase();
        createSimpleIndex(index);
        tEnv.executeSql(
                "CREATE TABLE es_table("
                        + "  id BIGINT,"
                        + "  label STRING,"
                        + "  vector ARRAY<FLOAT>"
                        + ")\n WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "elasticsearch-7")
                        + String.format(
                                "'%s'='%s',\n",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        + String.format(
                                "'%s'='%s'\n",
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                ES_CONTAINER.getHttpHostAddress())
                        + ")");

        tEnv.fromValues(
                        row(1L, "Batch", new Float[] {5f, 12f, 13f}),
                        row(2L, "Streaming", new Float[] {-5f, -12f, -13f}),
                        row(3L, "Big Data", new Float[] {1f, 1f, 0f}))
                .executeInsert("es_table")
                .await();

        // Wait for es construct index.
        Thread.sleep(2000);

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
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        mappingBuilder.startObject("properties");

        // id: long
        mappingBuilder.startObject("id");
        mappingBuilder.field("type", "long");
        mappingBuilder.endObject();

        // f1: string
        mappingBuilder.startObject("f1");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject();

        // f2: boolean
        mappingBuilder.startObject("f2");
        mappingBuilder.field("type", "boolean");
        mappingBuilder.endObject();

        // f3: tinyint
        mappingBuilder.startObject("f3");
        mappingBuilder.field("type", "byte");
        mappingBuilder.endObject();

        // f4: long
        mappingBuilder.startObject("f4");
        mappingBuilder.field("type", "short");
        mappingBuilder.endObject();

        // f5: long
        mappingBuilder.startObject("f5");
        mappingBuilder.field("type", "integer");
        mappingBuilder.endObject();

        // f6: date
        mappingBuilder.startObject("f6");
        mappingBuilder.field("type", "date");
        mappingBuilder.endObject();

        // f7: timestamp
        mappingBuilder.startObject("f7");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject();

        // f8: float
        mappingBuilder.startObject("f8");
        mappingBuilder.field("type", "float");
        mappingBuilder.endObject();

        // f9: double
        mappingBuilder.startObject("f9");
        mappingBuilder.field("type", "double");
        mappingBuilder.endObject();

        // f10: Array<Float>
        mappingBuilder.startObject("f10");
        mappingBuilder.field("type", "dense_vector");
        mappingBuilder.field("dims", 2);
        mappingBuilder.endObject();

        // f11: Array<Double>
        mappingBuilder.startObject("f11");
        mappingBuilder.field("type", "dense_vector");
        mappingBuilder.field("dims", 2);
        mappingBuilder.endObject();

        // f12: Array<Integer>
        mappingBuilder.startObject("f12");
        mappingBuilder.field("type", "dense_vector");
        mappingBuilder.field("dims", 2);
        mappingBuilder.endObject();

        // f13: Array<Long>
        mappingBuilder.startObject("f13");
        mappingBuilder.field("type", "dense_vector");
        mappingBuilder.field("dims", 2);
        mappingBuilder.endObject();

        mappingBuilder.endObject(); // end properties
        mappingBuilder.endObject(); // end root

        CreateIndexRequest request = new CreateIndexRequest(index);
        request.mapping(mappingBuilder);

        this.getClient().indices().create(request, RequestOptions.DEFAULT);
    }

    private void createSimpleIndex(String index) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        mappingBuilder.startObject("properties");

        // id: long
        mappingBuilder.startObject("id");
        mappingBuilder.field("type", "long");
        mappingBuilder.endObject();

        // f1: string
        mappingBuilder.startObject("label");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject();

        // f2: float vector
        mappingBuilder.startObject("vector");
        mappingBuilder.field("type", "dense_vector");
        mappingBuilder.field("dims", 3);
        mappingBuilder.endObject();

        mappingBuilder.endObject(); // end properties
        mappingBuilder.endObject(); // end root

        CreateIndexRequest request = new CreateIndexRequest(index);
        request.mapping(mappingBuilder);

        this.getClient().indices().create(request, RequestOptions.DEFAULT);
    }
}
