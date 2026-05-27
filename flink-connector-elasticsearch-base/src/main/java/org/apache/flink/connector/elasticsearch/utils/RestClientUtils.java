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

package org.apache.flink.connector.elasticsearch.utils;

import org.apache.flink.connector.elasticsearch.NetworkClientConfig;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClientBuilder;

/** Utils for RestClient. */
public class RestClientUtils {
    public static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig networkClientConfig) {
        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }

        final CredentialsProvider credentialsProvider = getCredentialsProvider(networkClientConfig);
        if (credentialsProvider != null
                || networkClientConfig.getSSLContextSupplier() != null
                || networkClientConfig.getSslHostnameVerifier() != null) {
            builder.setHttpClientConfigCallback(
                    httpClientBuilder -> {
                        if (credentialsProvider != null) {
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }

                        if (networkClientConfig.getSSLContextSupplier() != null) {
                            // client creates SSL context using the configured supplier
                            httpClientBuilder.setSSLContext(
                                    networkClientConfig.getSSLContextSupplier().get());
                        }

                        if (networkClientConfig.getSslHostnameVerifier() != null) {
                            httpClientBuilder.setSSLHostnameVerifier(
                                    networkClientConfig.getSslHostnameVerifier().get());
                        }

                        return httpClientBuilder;
                    });
        }

        if (networkClientConfig.getConnectionRequestTimeout() != null
                || networkClientConfig.getConnectionTimeout() != null
                || networkClientConfig.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (networkClientConfig.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    networkClientConfig.getConnectionRequestTimeout());
                        }
                        if (networkClientConfig.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    networkClientConfig.getConnectionTimeout());
                        }
                        if (networkClientConfig.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(
                                    networkClientConfig.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }

    /**
     * Get an http client credentials provider given network client config.
     *
     * <p>If network client config is not configured with username or password, return null.
     */
    private static CredentialsProvider getCredentialsProvider(
            NetworkClientConfig networkClientConfig) {
        CredentialsProvider credentialsProvider = null;
        if (networkClientConfig.getPassword() != null
                && networkClientConfig.getUsername() != null) {
            credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            networkClientConfig.getUsername(), networkClientConfig.getPassword()));
        }
        return credentialsProvider;
    }
}
