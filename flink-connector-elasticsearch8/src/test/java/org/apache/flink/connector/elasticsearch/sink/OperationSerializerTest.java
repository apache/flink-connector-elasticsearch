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

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** OperationSerializer tests. */
public class OperationSerializerTest {
    @ParameterizedTest
    @MethodSource("operations")
    public void testSerializeAndDeserialize(BulkOperationVariant operationVariant) {
        Operation expectedState = new Operation(operationVariant);

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bytes);

        new OperationSerializer().serialize(expectedState, out);

        final ByteArrayInputStream inputBytes = new ByteArrayInputStream(bytes.toByteArray());
        final DataInputStream in = new DataInputStream(inputBytes);

        Operation actualState =
                new OperationSerializer().deserialize(getRequestSize(expectedState), in);

        assertThat(actualState.getBulkOperationVariant())
                .usingRecursiveComparison(RecursiveComparisonConfiguration.builder().build())
                .isEqualTo(expectedState.getBulkOperationVariant());
    }

    private static Stream<Arguments> operations() {
        return Stream.of(
                Arguments.of(new DeleteOperation.Builder().id("delete").build()),
                Arguments.of(
                        new IndexOperation.Builder<>()
                                .id("index")
                                .index("testing")
                                .document(Collections.singletonMap("action", "index"))
                                .build()),
                Arguments.of(
                        new UpdateOperation.Builder<>()
                                .id("update")
                                .action(a -> a.doc(Collections.singletonMap("action", "update")))
                                .index("testing")
                                .build()));
    }

    private int getRequestSize(Operation requestEntry) {
        return new OperationSerializer().size(requestEntry);
    }
}
