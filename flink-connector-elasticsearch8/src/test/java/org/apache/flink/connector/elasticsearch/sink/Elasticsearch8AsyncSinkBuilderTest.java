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

import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link Elasticsearch8AsyncSinkBuilder}. */
public class Elasticsearch8AsyncSinkBuilderTest {
    @Test
    void testThrowExceptionIfElementConverterIsNotProvided() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setHosts(new HttpHost("localhost", 9200))
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfHostsAreNotProvided() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setElementConverter(
                                                (element, ctx) ->
                                                        Arrays.asList(
                                                                new DeleteOperation.Builder()
                                                                        .id("test")
                                                                        .index("test")
                                                                        .build()))
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfHostsIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setHosts(null)
                                        .setElementConverter(
                                                (element, ctx) ->
                                                        Arrays.asList(
                                                                new DeleteOperation.Builder()
                                                                        .id("test")
                                                                        .index("test")
                                                                        .build()))
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfUsernameIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setUsername(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfPasswordIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setPassword(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfHeadersAreNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setHeaders(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowExceptionIfCertificateFingerprintIsNull() {
        assertThatThrownBy(
                        () ->
                                Elasticsearch8AsyncSinkBuilder.<String>builder()
                                        .setCertificateFingerprint(null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }
}
