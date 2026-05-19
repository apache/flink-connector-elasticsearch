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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** IT tests for {@link ElasticSearch8AsyncDynamicSink}. */
@ExtendWith(ParameterizedTestExtension.class)
class Elasticsearch8DynamicSinkITCase extends Elasticsearch8DynamicSinkBaseITCase {
    private static final int PARALLELISM = 4;

    @TestTemplate
    public void testWritingDocuments() throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.TIME()),
                                Column.physical("c", DataTypes.STRING().notNull()),
                                Column.physical("d", DataTypes.FLOAT()),
                                Column.physical("e", DataTypes.TINYINT().notNull()),
                                Column.physical("f", DataTypes.DATE()),
                                Column.physical("g", DataTypes.TIMESTAMP().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("name", Arrays.asList("a", "g")));
        GenericRowData rowData =
                GenericRowData.of(
                        1L,
                        12345,
                        StringData.fromString("ABCDE"),
                        12.12f,
                        (byte) 2,
                        12345,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2012-12-12T12:12:12")));

        String index = "writing-documents";
        Elasticsearch8DynamicTableFactory sinkFactory = new Elasticsearch8DynamicTableFactory();

        DynamicTableSink.SinkRuntimeProvider runtimeProvider =
                sinkFactory
                        .createDynamicTableSink(
                                getPrefilledTestContext(index).withSchema(schema).build())
                        .getSinkRuntimeProvider(new MockContext());

        final SinkV2Provider sinkProvider = (SinkV2Provider) runtimeProvider;
        final Sink<RowData> sink = sinkProvider.createSink();
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(PARALLELISM);

        rowData.setRowKind(RowKind.UPDATE_AFTER);
        environment.<RowData>fromData(rowData).sinkTo(sink);
        environment.execute();

        Map<String, Object> response = makeGetRequest(index, "1_2012-12-12T12:12:12");
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    @TestTemplate
    public void testWritingDocumentsFromTableApi() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "table-api";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIME,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL,\n"
                        + "h as a + 2,\n"
                        + "PRIMARY KEY (a, g) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        Map<String, Object> response = makeGetRequest(index, "1_2012-12-12T12:12:12");
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    @TestTemplate
    public void testWritingDocumentsWithoutPrimaryKey() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "no-primary-key";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIME,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")),
                        row(
                                2L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "FGHIJK",
                                13.13f,
                                (byte) 4,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2013-12-12T13:13:13")))
                .executeInsert("esTable")
                .await();

        // search API does not return documents that were not indexed, we might need to query
        // the index a few times
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        List<Map<String, Object>> hits;
        do {
            hits = makeSearchRequest(index);
            if (hits.size() < 2) {
                Thread.sleep(200);
            }
        } while (hits.size() < 2 && deadline.hasTimeLeft());

        if (hits.size() < 2) {
            throw new AssertionError("Could not retrieve results from Elasticsearch.");
        }

        HashSet<Map<String, Object>> resultSet = new HashSet<>();
        resultSet.add(hits.get(0));
        resultSet.add(hits.get(1));
        Map<Object, Object> expectedMap1 = new HashMap<>();
        expectedMap1.put("a", 1);
        expectedMap1.put("b", "00:00:12");
        expectedMap1.put("c", "ABCDE");
        expectedMap1.put("d", 12.12d);
        expectedMap1.put("e", 2);
        expectedMap1.put("f", "2003-10-20");
        expectedMap1.put("g", "2012-12-12 12:12:12");
        Map<Object, Object> expectedMap2 = new HashMap<>();
        expectedMap2.put("a", 2);
        expectedMap2.put("b", "00:00:12");
        expectedMap2.put("c", "FGHIJK");
        expectedMap2.put("d", 13.13d);
        expectedMap2.put("e", 4);
        expectedMap2.put("f", "2003-10-20");
        expectedMap2.put("g", "2013-12-12 13:13:13");
        HashSet<Map<Object, Object>> expectedSet = new HashSet<>();
        expectedSet.add(expectedMap1);
        expectedSet.add(expectedMap2);
        assertThat(resultSet).isEqualTo(expectedSet);
    }

    @TestTemplate
    public void testUpdateOrDeleteWithoutPrimaryKey() {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "no-primary-key";

        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a INT NOT NULL,\n"
                        + "b STRING NOT NULL\n"
                        + ") WITH ("
                        + getConnectorSql(index)
                        + ")");

        String updateDataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 1, "a"),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 1, "a"),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2, "a")));

        String deleteDataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 3, "c"),
                                Row.ofKind(RowKind.DELETE, 3, "c")));

        tableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE updateOps ("
                                + "a INT NOT NULL,\n"
                                + "b STRING NOT NULL\n"
                                + ") WITH ("
                                + "'connector' = 'values', "
                                + "'data-id' = '%s',\n"
                                + "'changelog-mode' = 'I,UA,UB'"
                                + ")",
                        updateDataId));

        tableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE deleteOps ("
                                + "a INT NOT NULL,\n"
                                + "b STRING NOT NULL\n"
                                + ") WITH ("
                                + "'connector' = 'values', "
                                + "'data-id' = '%s',\n"
                                + "'changelog-mode' = 'I,D'"
                                + ")",
                        deleteDataId));

        String expectedExceptionMsg =
                "Primary key is required when the changelog contains UPDATE or DELETE operations.";

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(
                                                "INSERT INTO esTable SELECT a, b FROM updateOps")
                                        .await())
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedExceptionMsg);

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(
                                                "INSERT INTO esTable SELECT a, b FROM deleteOps")
                                        .await())
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedExceptionMsg);
    }

    @TestTemplate
    public void testWritingDocumentsWithDynamicIndex() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "dynamic-index-{b|yyyy-MM-dd}";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP NOT NULL,\n"
                        + "PRIMARY KEY (a) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        tableEnvironment
                .fromValues(row(1L, LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        Map<String, Object> response = makeGetRequest("dynamic-index-2012-12-12", "1");
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    @TestTemplate
    public void testWritingDocumentsWithDynamicIndexFromSystemTime() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        tableEnvironment.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "Asia/Shanghai");

        String dynamicIndex1 =
                "dynamic-index-"
                        + dateTimeFormatter.format(LocalDateTime.now(ZoneId.of("Asia/Shanghai")))
                        + "_index";

        String index = "dynamic-index-{now()|yyyy-MM-dd}_index";

        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP NOT NULL,\n"
                        + "PRIMARY KEY (a) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        String dynamicIndex2 =
                "dynamic-index-"
                        + dateTimeFormatter.format(LocalDateTime.now(ZoneId.of("Asia/Shanghai")))
                        + "_index";

        tableEnvironment
                .fromValues(row(1L, LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        Map<String, Object> response;
        try {
            response = makeGetRequest(dynamicIndex1, "1");
        } catch (Exception e) {
            if (e.getMessage().contains("index_not_found_exception")) {
                response = makeGetRequest(dynamicIndex2, "1");
            } else {
                throw e;
            }
        }

        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);

        expectedMap.put("b", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }
}
