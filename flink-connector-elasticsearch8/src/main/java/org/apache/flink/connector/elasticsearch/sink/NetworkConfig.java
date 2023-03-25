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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** NetworkConfigConfig A factory that creates valid ElasticsearchClient instances. */
public class NetworkConfig {
    private final List<HttpHost> hosts;

    private final List<Header> headers;

    private final String username;

    private final String password;

    private final String certificateFingerprint;

    public NetworkConfig(
            List<HttpHost> hosts,
            String username,
            String password,
            List<Header> headers,
            String certificateFingerprint) {
        checkState(hosts.size() > 0, "Hosts must not be null");
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.headers = headers;
        this.certificateFingerprint = certificateFingerprint;
    }

    public ElasticsearchAsyncClient create() {
        return new ElasticsearchAsyncClient(
                new RestClientTransport(this.getRestClient(), new JacksonJsonpMapper()));
    }

    private RestClient getRestClient() {
        RestClientBuilder restClientBuilder =
                RestClient.builder(hosts.toArray(new HttpHost[0]))
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> {
                                    if (username != null && password != null) {
                                        httpClientBuilder.setDefaultCredentialsProvider(
                                                getCredentials());
                                    }

                                    if (certificateFingerprint != null) {
                                        httpClientBuilder.setSSLContext(
                                                TransportUtils.sslContextFromCaFingerprint(
                                                        certificateFingerprint));
                                    }

                                    return httpClientBuilder;
                                });

        if (headers != null) {
            restClientBuilder.setDefaultHeaders(headers.toArray(new Header[0]));
        }

        return restClientBuilder.build();
    }

    private CredentialsProvider getCredentials() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        return credentialsProvider;
    }
}
