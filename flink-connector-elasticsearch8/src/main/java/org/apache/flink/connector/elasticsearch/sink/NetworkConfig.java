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

import org.apache.flink.util.function.SerializableSupplier;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalTimeSerializer;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** A factory that creates valid ElasticsearchClient instances. */
public class NetworkConfig implements Serializable {
    private final List<HttpHost> hosts;
    private final List<Header> headers;
    private final String username;
    private final String password;
    @Nullable private final String connectionPathPrefix;
    @Nullable Integer connectionRequestTimeout;
    @Nullable Integer connectionTimeout;
    @Nullable Integer socketTimeout;
    @Nullable private final SerializableSupplier<SSLContext> sslContextSupplier;
    @Nullable private final SerializableSupplier<HostnameVerifier> sslHostnameVerifier;
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    public NetworkConfig(
            List<HttpHost> hosts,
            String username,
            String password,
            List<Header> headers,
            @Nullable String connectionPathPrefix,
            @Nullable Integer connectionRequestTimeout,
            @Nullable Integer connectionTimeout,
            @Nullable Integer socketTimeout,
            @Nullable SerializableSupplier<SSLContext> sslContextSupplier,
            @Nullable SerializableSupplier<HostnameVerifier> sslHostnameVerifier) {
        checkState(!hosts.isEmpty(), "Hosts must not be empty");
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.headers = headers;
        this.connectionRequestTimeout = connectionRequestTimeout;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.connectionPathPrefix = connectionPathPrefix;
        this.sslContextSupplier = sslContextSupplier;
        this.sslHostnameVerifier = sslHostnameVerifier;
    }

    public ElasticsearchAsyncClient createEsClient() {
        // the JavaTimeModule is added to provide support for java 8 Time classes.
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(
                LocalDateTime.class, new LocalDateTimeSerializer(DATE_TIME_FORMATTER));
        javaTimeModule.addSerializer(LocalDate.class, new LocalDateSerializer(DATE_FORMATTER));
        javaTimeModule.addSerializer(LocalTime.class, new LocalTimeSerializer(TIME_FORMATTER));
        ObjectMapper mapper = JsonMapper.builder().addModule(javaTimeModule).build();
        return new ElasticsearchAsyncClient(
                new RestClientTransport(this.getRestClient(), new JacksonJsonpMapper(mapper)));
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

                                    if (sslContextSupplier != null) {
                                        httpClientBuilder.setSSLContext(sslContextSupplier.get());
                                    }

                                    if (sslHostnameVerifier != null) {
                                        httpClientBuilder.setSSLHostnameVerifier(
                                                sslHostnameVerifier.get());
                                    }

                                    return httpClientBuilder;
                                });

        if (headers != null) {
            restClientBuilder.setDefaultHeaders(headers.toArray(new Header[0]));
        }

        if (connectionPathPrefix != null) {
            restClientBuilder.setPathPrefix(connectionPathPrefix);
        }

        if (connectionRequestTimeout != null
                || connectionTimeout != null
                || socketTimeout != null) {
            restClientBuilder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (connectionRequestTimeout != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    connectionRequestTimeout);
                        }
                        if (connectionTimeout != null) {
                            requestConfigBuilder.setConnectTimeout(connectionTimeout);
                        }
                        if (socketTimeout != null) {
                            requestConfigBuilder.setSocketTimeout(socketTimeout);
                        }
                        return requestConfigBuilder;
                    });
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
