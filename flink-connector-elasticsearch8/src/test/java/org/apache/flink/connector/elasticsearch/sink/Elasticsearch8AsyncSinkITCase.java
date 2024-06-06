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
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Integration tests for {@link Elasticsearch8AsyncSink}. */
public class Elasticsearch8AsyncSinkITCase extends ElasticsearchSinkBaseITCase {
    private static boolean failed;

    @BeforeEach
    void setUp() {
        failed = false;
    }

    @TestTemplate
    public void testWriteToElasticsearch() throws Exception {
        String index = "test-write-to-elasticsearch";

        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1)) {
            env.setRestartStrategy(RestartStrategies.noRestart());

            Elasticsearch8AsyncSink<DummyData> sink = getSinkForDummyData(index);

            env.fromElements("first", "second", "third", "fourth", "fifth")
                    .map(
                            (MapFunction<String, DummyData>)
                                    value -> new DummyData(value + "_v1_index", value))
                    .sinkTo(sink);

            env.execute();
        }

        assertIdsAreWritten(index, new String[] {"first_v1_index", "second_v1_index"});
    }

    @TestTemplate
    public void testRecovery() throws Exception {
        String index = "test-recovery";

        try (final StreamExecutionEnvironment env = new LocalStreamEnvironment()) {
            env.enableCheckpointing(100L);

            final Elasticsearch8AsyncSink<DummyData> sink = getSinkForDummyData(index);

            env.fromElements("first", "second", "third", "fourth", "fifth")
                    .map(
                            (MapFunction<String, DummyData>)
                                    value -> new DummyData(value + "_v1_index", value))
                    .map(new BuggyMapper())
                    .sinkTo(sink);

            env.execute();
        }

        assertThat(failed).isEqualTo(true);
    }

    private static class BuggyMapper
            implements MapFunction<DummyData, DummyData>, CheckpointListener {
        private int emittedRecords = 0;

        @Override
        public DummyData map(DummyData dummyData) throws InterruptedException {
            Thread.sleep(50);
            emittedRecords++;
            return dummyData;
        }

        @Override
        public void notifyCheckpointComplete(long l) throws Exception {
            if (!failed || emittedRecords != 0) {
                failed = true;
                throw new Exception();
            }
        }
    }
}
