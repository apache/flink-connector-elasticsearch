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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import org.apache.http.Header;
import org.apache.http.HttpHost;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch8AsyncSinkBuilder The builder to construct the Elasticsearch8Sink {@link
 * Elasticsearch8AsyncSink}.
 *
 * @param <InputT> the type of records to be sunk into an Elasticsearch cluster
 */
public class Elasticsearch8AsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, Operation, Elasticsearch8AsyncSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1024 * 1024;

    /** The hosts where the Elasticsearch cluster is reachable. */
    private List<HttpHost> hosts;

    /** The headers to be sent with the requests made to Elasticsearch cluster. */
    private List<Header> headers;

    /** The Certificate Fingerprint will be used to verify the HTTPS connection. */
    private String certificateFingerprint;

    /** The username to authenticate the connection with the Elasticsearch cluster. */
    private String username;

    /** The password to authenticate the connection with the Elasticsearch cluster. */
    private String password;

    /**
     * The element converter that will be called on every stream element to be processed and
     * buffered.
     */
    private ElementConverter<InputT, BulkOperationVariant> elementConverter;

    /**
     * setHosts set the hosts where the Elasticsearch cluster is reachable.
     *
     * @param hosts the hosts address
     * @return {@code Elasticsearch8AsyncSinkBuilder}
     */
    public Elasticsearch8AsyncSinkBuilder<InputT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkArgument(hosts.length > 0, "Hosts cannot be empty");
        this.hosts = Arrays.asList(hosts);
        return this;
    }

    /**
     * setHeaders set the headers to be sent with the requests made to Elasticsearch cluster..
     *
     * @param headers the headers
     * @return {@code Elasticsearch8AsyncSinkBuilder}
     */
    public Elasticsearch8AsyncSinkBuilder<InputT> setHeaders(Header... headers) {
        checkNotNull(hosts);
        checkArgument(headers.length > 0, "Hosts cannot be empty");
        this.headers = Arrays.asList(headers);
        return this;
    }

    /**
     * setCertificateFingerprint set the certificate fingerprint to be used to verify the HTTPS
     * connection.
     *
     * @param certificateFingerprint the certificate fingerprint
     * @return {@code Elasticsearch8AsyncSinkBuilder}
     */
    public Elasticsearch8AsyncSinkBuilder<InputT> setCertificateFingerprint(
            String certificateFingerprint) {
        checkNotNull(username, "certificateFingerprint must not be null");
        this.certificateFingerprint = certificateFingerprint;
        return this;
    }

    /**
     * setUsername set the username to authenticate the connection with the Elasticsearch cluster.
     *
     * @param username the auth username
     * @return {@code Elasticsearch8AsyncSinkBuilder}
     */
    public Elasticsearch8AsyncSinkBuilder<InputT> setUsername(String username) {
        checkNotNull(username, "Username must not be null");
        this.username = username;
        return this;
    }

    /**
     * setPassword set the password to authenticate the connection with the Elasticsearch cluster.
     *
     * @param password the auth password
     * @return {@code Elasticsearch8AsyncSinkBuilder}
     */
    public Elasticsearch8AsyncSinkBuilder<InputT> setPassword(String password) {
        checkNotNull(password, "Password must not be null");
        this.password = password;
        return this;
    }

    /**
     * setElementConverter set the element converter that will be called at every stream element to
     * be processed and buffered.
     *
     * @param elementConverter elementConverter operation
     * @return {@code Elasticsearch8AsyncSinkBuilder}
     */
    public Elasticsearch8AsyncSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, BulkOperationVariant> elementConverter) {
        checkNotNull(elementConverter);
        this.elementConverter = elementConverter;
        return this;
    }

    public static <T> Elasticsearch8AsyncSinkBuilder<T> builder() {
        return new Elasticsearch8AsyncSinkBuilder<>();
    }

    /**
     * Creates an ElasticsearchSink instance.
     *
     * @return {@link Elasticsearch8AsyncSink}
     */
    @Override
    public Elasticsearch8AsyncSink<InputT> build() {
        return new Elasticsearch8AsyncSink<>(
                buildOperationConverter(elementConverter),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                username,
                password,
                certificateFingerprint,
                hosts,
                headers);
    }

    private OperationConverter<InputT> buildOperationConverter(
            ElementConverter<InputT, BulkOperationVariant> converter) {
        return converter != null ? new OperationConverter<>(converter) : null;
    }

    /** A wrapper that evolves the Operation, since a BulkOperationVariant is not Serializable. */
    public static class OperationConverter<T> implements ElementConverter<T, Operation> {
        private final ElementConverter<T, BulkOperationVariant> converter;

        public OperationConverter(ElementConverter<T, BulkOperationVariant> converter) {
            this.converter = converter;
        }

        @Override
        public Operation apply(T element, SinkWriter.Context context) {
            return new Operation(converter.apply(element, context));
        }
    }
}
