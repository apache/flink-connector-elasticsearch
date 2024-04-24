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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.elasticsearch.sink.BulkResponseInspector.BulkResponseInspectorFactory;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchWriter.DefaultBulkResponseInspector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ElasticsearchSinkBuilderBase}. */
@ExtendWith(TestLoggerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ElasticsearchSinkBuilderBaseTest<B extends ElasticsearchSinkBuilderBase<Object, B>> {

    @TestFactory
    Stream<DynamicTest> testValidBuilders() {
        Stream<B> validBuilders =
                Stream.of(
                        createMinimalBuilder(),
                        createMinimalBuilder()
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE),
                        createMinimalBuilder()
                                .setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 1, 1),
                        createMinimalBuilder()
                                .setConnectionUsername("username")
                                .setConnectionPassword("password"),
                        createMinimalBuilder().allowInsecure());

        return DynamicTest.stream(
                validBuilders,
                ElasticsearchSinkBuilderBase::toString,
                builder -> assertThatCode(builder::build).doesNotThrowAnyException());
    }

    @Test
    void testDefaultDeliveryGuarantee() {
        assertThat(createMinimalBuilder().build().getDeliveryGuarantee())
                .isEqualTo(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    @Test
    void testAllowInsecureSetSslContextSupplier() {
        assertThat(
                        createMinimalBuilder()
                                .allowInsecure()
                                .build()
                                .getNetworkClientConfig()
                                .getSSLContextSupplier())
                .isNotNull();
    }

    @Test
    void testThrowIfExactlyOnceConfigured() {
        assertThatThrownBy(
                        () ->
                                createMinimalBuilder()
                                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testThrowIfHostsNotSet() {
        assertThatThrownBy(
                        () ->
                                createEmptyBuilder()
                                        .setEmitter((element, indexer, context) -> {})
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfEmitterNotSet() {
        assertThatThrownBy(
                        () -> createEmptyBuilder().setHosts(new HttpHost("localhost:3000")).build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfSetInvalidTimeouts() {
        assertThatThrownBy(() -> createEmptyBuilder().setConnectionRequestTimeout(-1).build())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> createEmptyBuilder().setConnectionTimeout(-1).build())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> createEmptyBuilder().setSocketTimeout(-1).build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOverrideFailureHandler() {
        final FailureHandler failureHandler = (failure) -> {};
        final ElasticsearchSink<Object> sink =
                createMinimalBuilder().setFailureHandler(failureHandler).build();

        final TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        final BulkResponseInspector bulkResponseInspector =
                sink.getBulkResponseInspectorFactory().apply(sinkInitContext::metricGroup);
        assertThat(bulkResponseInspector)
                .isInstanceOf(DefaultBulkResponseInspector.class)
                .extracting(
                        (inspector) -> ((DefaultBulkResponseInspector) inspector).failureHandler)
                .isEqualTo(failureHandler);
    }

    @Test
    void testOverrideBulkResponseInspectorFactory() {
        final AtomicBoolean called = new AtomicBoolean();
        final BulkResponseInspectorFactory bulkResponseInspectorFactory =
                initContext -> {
                    final MetricGroup metricGroup = initContext.metricGroup();
                    metricGroup.addGroup("bulk").addGroup("result", "failed").counter("actions");
                    called.set(true);
                    return (BulkResponseInspector) (request, response) -> {};
                };
        final ElasticsearchSink<Object> sink =
                createMinimalBuilder()
                        .setBulkResponseInspectorFactory(bulkResponseInspectorFactory)
                        .build();

        final TestSinkInitContext sinkInitContext = new TestSinkInitContext();

        assertThatCode(() -> sink.createWriter(sinkInitContext)).doesNotThrowAnyException();
        assertThat(called).isTrue();
    }

    abstract B createEmptyBuilder();

    abstract B createMinimalBuilder();
}
