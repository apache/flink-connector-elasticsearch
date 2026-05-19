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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.api.ValidationException;

import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_BUFFERED_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_IN_FLIGHT_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.CONNECTION_PATH_PREFIX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.CONNECTION_REQUEST_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DELIVERY_GUARANTEE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.MAX_RETRIES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.NUM_CANDIDATES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SOCKET_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_CERTIFICATE_FINGERPRINT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Elasticsearch base configuration. */
@Internal
public class Elasticsearch8Configuration {
    protected final ReadableConfig config;

    Elasticsearch8Configuration(ReadableConfig config) {
        this.config = checkNotNull(config);
    }

    public int getBulkFlushMaxActions() {
        return config.get(BULK_FLUSH_MAX_ACTIONS_OPTION);
    }

    public int getBulkFlushMaxBufferedActions() {
        return config.get(BULK_FLUSH_MAX_BUFFERED_ACTIONS_OPTION);
    }

    public int getBulkFlushMaxInFlightActions() {
        return config.get(BULK_FLUSH_MAX_IN_FLIGHT_ACTIONS_OPTION);
    }

    public MemorySize getBulkFlushMaxByteSize() {
        return config.get(BULK_FLUSH_MAX_SIZE_OPTION);
    }

    public long getBulkFlushInterval() {
        return config.get(BULK_FLUSH_INTERVAL_OPTION).toMillis();
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return config.get(DELIVERY_GUARANTEE_OPTION);
    }

    public Optional<String> getUsername() {
        return config.getOptional(USERNAME_OPTION);
    }

    public Optional<String> getPassword() {
        return config.getOptional(PASSWORD_OPTION);
    }

    public String getIndex() {
        return config.get(INDEX_OPTION);
    }

    public String getKeyDelimiter() {
        return config.get(KEY_DELIMITER_OPTION);
    }

    public Optional<String> getPathPrefix() {
        return config.getOptional(CONNECTION_PATH_PREFIX_OPTION);
    }

    public Optional<Duration> getConnectionRequestTimeout() {
        return config.getOptional(CONNECTION_REQUEST_TIMEOUT);
    }

    public Optional<Duration> getConnectionTimeout() {
        return config.getOptional(CONNECTION_TIMEOUT);
    }

    public Optional<Duration> getSocketTimeout() {
        return config.getOptional(SOCKET_TIMEOUT);
    }

    public Optional<String> getCertificateFingerprint() {
        return config.getOptional(SSL_CERTIFICATE_FINGERPRINT);
    }

    public List<HttpHost> getHosts() {
        return config.get(HOSTS_OPTION).stream()
                .map(Elasticsearch8Configuration::validateAndParseHostsString)
                .collect(Collectors.toList());
    }

    public Optional<Integer> getParallelism() {
        return config.getOptional(SINK_PARALLELISM);
    }

    // --- Lookup / vector search accessors --------------------------------------------------

    public int getMaxRetries() {
        return config.get(MAX_RETRIES);
    }

    public int getNumCandidates() {
        return config.get(NUM_CANDIDATES);
    }

    /**
     * Parse Hosts String to list.
     *
     * <p>Hosts String format was given as following:
     *
     * <pre>
     *     connector.hosts = http://host_name:9092;http://host_name:9093
     * </pre>
     */
    public static HttpHost validateAndParseHostsString(String host) {
        try {
            HttpHost httpHost = HttpHost.create(host);
            if (httpHost.getPort() < 0) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
                                host, HOSTS_OPTION.key()));
            }

            if (httpHost.getSchemeName() == null) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
                                host, HOSTS_OPTION.key()));
            }
            return httpHost;
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
                            host, HOSTS_OPTION.key()),
                    e);
        }
    }
}
