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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.connector.elasticsearch.sink.ElasticsearchWriter.DefaultBulkResponseInspector;
import org.apache.flink.util.FlinkRuntimeException;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class DefaultBulkResponseInspectorTest {

    @Test
    void testPassWithoutFailures() {
        final DefaultBulkResponseInspector inspector = new DefaultBulkResponseInspector();
        Assertions.assertThatCode(
                        () ->
                                inspector.inspect(
                                        new BulkRequest(),
                                        new BulkResponse(new BulkItemResponse[] {}, 0)))
                .doesNotThrowAnyException();
    }

    @Test
    void testPassesDespiteChainedFailure() {
        final DefaultBulkResponseInspector inspector =
                new DefaultBulkResponseInspector((failure) -> {});
        Assertions.assertThatCode(
                        () -> {
                            final BulkRequest request = new BulkRequest();
                            request.add(
                                    new IndexRequest(), new DeleteRequest(), new DeleteRequest());

                            inspector.inspect(
                                    request,
                                    new BulkResponse(
                                            new BulkItemResponse[] {
                                                new BulkItemResponse(
                                                        0, OpType.CREATE, (DocWriteResponse) null),
                                                new BulkItemResponse(
                                                        1,
                                                        OpType.DELETE,
                                                        new Failure(
                                                                "index",
                                                                "type",
                                                                "id",
                                                                new IOException("A"))),
                                                new BulkItemResponse(
                                                        2,
                                                        OpType.DELETE,
                                                        new Failure(
                                                                "index",
                                                                "type",
                                                                "id",
                                                                new IOException("B")))
                                            },
                                            0));
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void testThrowsChainedFailure() {
        final IOException failureCause0 = new IOException("A");
        final IOException failureCause1 = new IOException("B");
        final DefaultBulkResponseInspector inspector = new DefaultBulkResponseInspector();
        Assertions.assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(
                        () -> {
                            final BulkRequest request = new BulkRequest();
                            request.add(
                                    new IndexRequest(), new DeleteRequest(), new DeleteRequest());

                            inspector.inspect(
                                    request,
                                    new BulkResponse(
                                            new BulkItemResponse[] {
                                                new BulkItemResponse(
                                                        0, OpType.CREATE, (DocWriteResponse) null),
                                                new BulkItemResponse(
                                                        1,
                                                        OpType.DELETE,
                                                        new Failure(
                                                                "index",
                                                                "type",
                                                                "id",
                                                                failureCause0)),
                                                new BulkItemResponse(
                                                        2,
                                                        OpType.DELETE,
                                                        new Failure(
                                                                "index",
                                                                "type",
                                                                "id",
                                                                failureCause1))
                                            },
                                            0));
                        })
                .withCause(failureCause0);
    }
}
