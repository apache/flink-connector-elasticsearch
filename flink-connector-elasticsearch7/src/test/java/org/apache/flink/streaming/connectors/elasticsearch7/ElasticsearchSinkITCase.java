/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.test.DockerImageVersions;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticSearchInputFormatBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** IT cases for the {@link ElasticsearchSink}. */
public class ElasticsearchSinkITCase<T>
        extends ElasticsearchSinkTestBase<RestHighLevelClient, HttpHost> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkITCase.class);

    @ClassRule
    public static ElasticsearchContainer elasticsearchContainer =
            ElasticsearchUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_7, LOG);

    @Override
    protected final RestHighLevelClient getClient() {
        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(elasticsearchContainer.getHttpHostAddress())));
    }

    @Test
    public void testElasticsearchSink() throws Exception {
        runElasticsearchSinkTest();
    }

    @Test
    public void testElasticsearchSinkWithSmile() throws Exception {
        runElasticsearchSinkSmileTest();
    }

    @Test
    public void testElasticsearchInputFormat() throws Exception {
        runElasticsearchSinkTest();
        runElasticSearchInputFormatTest();
    }

    @Test
    public void testNullAddresses() {
        runNullAddressesTest();
    }

    @Test
    public void testEmptyAddresses() {
        runEmptyAddressesTest();
    }

    @Test
    public void testInvalidElasticsearchCluster() throws Exception {
        runInvalidElasticsearchClusterTest();
    }

    @Override
    protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient>
            createElasticsearchSink(
            int bulkFlushMaxActions,
            List httpHosts,
            ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

        ElasticsearchSink.Builder<Tuple2<Integer, String>> builder =
                new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(bulkFlushMaxActions);

        return builder.build();
    }

    protected ElasticSearchInputFormatBase createElasticsearchInputFormat(
            Map<String, String> userConfig,
            DeserializationSchema<RestHighLevelClient> deserializationSchema,
            String[] fieldNames,
            String index,
            String type,
            long scrollTimeout,
            int scrollMaxSize,
            QueryBuilder predicate,
            int limit) throws Exception {
        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

        ElasticSearch7InputFormat builder = new ElasticSearch7InputFormat.Builder()
                .setDeserializationSchema(deserializationSchema)
                .setFieldNames(fieldNames)
                .setIndex(index)
                .setScrollTimeout(scrollTimeout)
                .setScrollMaxSize(scrollMaxSize)
                .setPredicate(predicate)
                .setLimit(limit)
                .build();
        return builder;
    }



    @Override
    protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient>
            createElasticsearchSinkForEmbeddedNode(
                    int bulkFlushMaxActions,
                    ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

        return createElasticsearchSinkForNode(
                bulkFlushMaxActions,
                elasticsearchSinkFunction,
                elasticsearchContainer.getHttpHostAddress());
    }

    @Override
    protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient>
            createElasticsearchSinkForNode(
                    int bulkFlushMaxActions,
                    ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction,
                    String hostAddress) {

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(HttpHost.create(hostAddress));

        ElasticsearchSink.Builder<Tuple2<Integer, String>> builder =
                new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(bulkFlushMaxActions);

        return builder.build();
    }
}
