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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Before;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/** Base class for end to end Elasticsearch lookup. */
public abstract class ElasticsearchLookupE2ECase {
    protected EnvironmentSettings streamSettings;
    protected static final String DIM = "testTable1";
    protected static final String ES_INDEX = "es1";
    // prepare a source collection.
    private static final List<Row> srcData = new ArrayList<>();
    private static final RowTypeInfo testTypeInfo =
            new RowTypeInfo(
                    new TypeInformation[] {Types.INT, Types.LONG, Types.STRING},
                    new String[] {"a", "b", "c"});

    ElasticsearchContainer elasticsearchContainer = null;

    static {
        srcData.add(Row.of(1, 1L, "Hi"));
        srcData.add(Row.of(2, 2L, "Hello"));
        srcData.add(Row.of(3, 2L, "Hello Yubin Li"));
        srcData.add(Row.of(4, 999L, "Hello Yubin Li !"));
    }

    abstract String getElasticsearchContainerName();

    abstract String getEsOptions();

    @Before
    public void before() {
        this.streamSettings = EnvironmentSettings.inStreamingMode();
        elasticsearchContainer = new ElasticsearchContainer(getElasticsearchContainerName());
        elasticsearchContainer.start();
    }

    @Test
    public void testEsLookupTableSource() {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        tEnv.executeSql(
                "CREATE TABLE "
                        + DIM
                        + " ("
                        + " id int,"
                        + " name string,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + getEsOptions()
                        + ")");
        tEnv.executeSql("insert into " + DIM + " values (1, 'rick')");
        tEnv.executeSql("insert into " + DIM + " values (2, 'john')");
        tEnv.executeSql("insert into " + DIM + " values (3, 'ted')");

        // prepare a source table
        String srcTableName = "src";
        DataStream<Row> srcDs = execEnv.fromCollection(srcData).returns(testTypeInfo);
        Table in = tEnv.fromDataStream(srcDs, $("a"), $("b"), $("c"), $("proc").proctime());
        tEnv.registerTable(srcTableName, in);

        // perform a temporal table join query
        String dimJoinQuery =
                "SELECT"
                        + " a,"
                        + " b,"
                        + " c,"
                        + " id,"
                        + " name"
                        + " FROM src JOIN "
                        + DIM
                        + " FOR SYSTEM_TIME AS OF src.proc as h ON src.a = h.id";
        Iterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I[1, 1, Hi, 1, rick]");
        expected.add("+I[2, 2, Hello, 2, john]");
        expected.add("+I[3, 2, Hello Yubin Li, 3, ted]");

        assertEquals(expected, result);
    }
}
