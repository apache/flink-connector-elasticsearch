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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.DockerImageVersions;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.RequestTest;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.Version;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.net.ssl.SSLContext;

/** Tests for {@link Elasticsearch8Sink}. */
@Testcontainers
class Elasticsearch8SinkITCase extends Elasticsearch8SinkBaseITCase {

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(DockerImageVersions.ELASTICSEARCH_8, LOG)
                    .withPassword(ELASTICSEARCH_PASSWORD);

    private static final Version version =
            DockerImageVersions.getVersion(DockerImageVersions.ELASTICSEARCH_8);

    private static final boolean useTLS = version.major() >= 8;

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
    TestClientBase createTestClient(ElasticsearchClient client) {
        return new Elasticsearch8TestClient(client);
    }

    @Override
    Elasticsearch8SinkBuilderBase<
                    Tuple2<Integer, RequestTest.Product>, ? extends Elasticsearch8SinkBuilderBase>
            getSinkBuilder() {
        return new Elasticsearch8SinkBuilder<>();
    }
}
