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

package org.apache.flink.connector.elasticsearch.server;

import org.apache.flink.connector.elasticsearch.DockerImageVersions;
import org.apache.flink.connector.elasticsearch.ElasticsearchServerBaseITCase;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.RequestTest;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Version;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.net.ssl.SSLContext;

import java.io.IOException;

/** Tests for elasticsearch 8 server and client. */
@Testcontainers
public class Elasticsearch8Test extends ElasticsearchServerBaseITCase {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8Test.class);

    protected static final String ELASTICSEARCH_PASSWORD = "test-password";

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
    public ElasticsearchClient createElasticsearch8Client() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD));
        HttpHost host = HttpHost.create(getElasticsearchHttpHostAddress());
        RestClient httpClient =
                RestClient.builder(host)
                        .setHttpClientConfigCallback(
                                hc ->
                                        hc.setDefaultCredentialsProvider(credentialsProvider)
                                                .setSSLContext(getSSLContext()))
                        .build();
        ElasticsearchTransport transport =
                new RestClientTransport(httpClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    @Test
    public void testServer() throws IOException {
        SSLContext sslContext = ES_CONTAINER.createSslContextFromCa();
        final BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
        credsProv.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials("elastic", ELASTICSEARCH_PASSWORD));
        HttpHost httpHost = HttpHost.create("https://" + ES_CONTAINER.getHttpHostAddress());
        RestClient restClient =
                RestClient.builder(httpHost)
                        .setHttpClientConfigCallback(
                                hc ->
                                        hc.setSSLContext(sslContext)
                                                .setDefaultCredentialsProvider(credsProv))
                        .build();

        // Note: client.helthReport() has some errors, we need to check in the future.
        // Create the transport with a Jackson mapper
        // ElasticsearchTransport transport =
        //       new RestClientTransport(restClient, new JacksonJsonpMapper());

        // And create the API client
        // ElasticsearchClient esClient = new ElasticsearchClient(transport);
        // esClient.healthReport();

        Request request = new Request("GET", "_cluster/health");
        Response resp = restClient.performRequest(request);
        Assert.assertEquals(200, resp.getStatusLine().getStatusCode());
    }

    @Test
    public void testClient() throws IOException {
        ElasticsearchClient esClient = createElasticsearch8Client();
        esClient.indices().create(c -> c.index("products"));
        esClient.index(i -> i.index("products").id("A").document(new RequestTest.Product(10)));
        GetResponse<RequestTest.Product> getResponse =
                esClient.get(
                        GetRequest.of(b -> b.index("products").id("A")), RequestTest.Product.class);
        Assert.assertTrue(getResponse.found());
    }
}
