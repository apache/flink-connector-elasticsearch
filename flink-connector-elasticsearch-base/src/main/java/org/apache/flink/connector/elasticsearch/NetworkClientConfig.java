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

package org.apache.flink.connector.elasticsearch;

import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.Serializable;

/** Network config for es client. */
public class NetworkClientConfig implements Serializable {

    @Nullable private final String username;
    @Nullable private final String password;
    @Nullable private final String connectionPathPrefix;
    @Nullable private final Integer connectionRequestTimeout;
    @Nullable private final Integer connectionTimeout;
    @Nullable private final Integer socketTimeout;
    @Nullable private final SerializableSupplier<SSLContext> sslContextSupplier;
    @Nullable private final SerializableSupplier<HostnameVerifier> sslHostnameVerifier;

    public NetworkClientConfig(
            @Nullable String username,
            @Nullable String password,
            @Nullable String connectionPathPrefix,
            @Nullable Integer connectionRequestTimeout,
            @Nullable Integer connectionTimeout,
            @Nullable Integer socketTimeout,
            @Nullable SerializableSupplier<SSLContext> sslContextSupplier,
            @Nullable SerializableSupplier<HostnameVerifier> sslHostnameVerifier) {
        this.username = username;
        this.password = password;
        this.connectionPathPrefix = connectionPathPrefix;
        this.connectionRequestTimeout = connectionRequestTimeout;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.sslContextSupplier = sslContextSupplier;
        this.sslHostnameVerifier = sslHostnameVerifier;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    @Nullable
    public Integer getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    @Nullable
    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    @Nullable
    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    @Nullable
    public String getConnectionPathPrefix() {
        return connectionPathPrefix;
    }

    @Nullable
    public SerializableSupplier<SSLContext> getSSLContextSupplier() {
        return sslContextSupplier;
    }

    @Nullable
    public SerializableSupplier<HostnameVerifier> getSslHostnameVerifier() {
        return sslHostnameVerifier;
    }

    /** Builder for {@link NetworkClientConfig}. */
    public static class Builder {
        private String username;
        private String password;
        private String connectionPathPrefix;
        private Integer connectionRequestTimeout;
        private Integer connectionTimeout;
        private Integer socketTimeout;
        private SerializableSupplier<SSLContext> sslContextSupplier;
        private SerializableSupplier<HostnameVerifier> sslHostnameVerifier;

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setConnectionPathPrefix(String connectionPathPrefix) {
            this.connectionPathPrefix = connectionPathPrefix;
            return this;
        }

        public Builder setConnectionRequestTimeout(Integer connectionRequestTimeout) {
            this.connectionRequestTimeout = connectionRequestTimeout;
            return this;
        }

        public Builder setConnectionTimeout(Integer connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setSocketTimeout(Integer socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        public Builder setSslContextSupplier(SerializableSupplier<SSLContext> sslContextSupplier) {
            this.sslContextSupplier = sslContextSupplier;
            return this;
        }

        public Builder setSslHostnameVerifier(
                SerializableSupplier<HostnameVerifier> sslHostnameVerifier) {
            this.sslHostnameVerifier = sslHostnameVerifier;
            return this;
        }

        public NetworkClientConfig build() {
            return new NetworkClientConfig(
                    username,
                    password,
                    connectionPathPrefix,
                    connectionRequestTimeout,
                    connectionTimeout,
                    socketTimeout,
                    sslContextSupplier,
                    sslHostnameVerifier);
        }
    }
}
