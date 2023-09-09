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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.RequestTest;
import org.apache.flink.connector.elasticsearch.bulk.RequestIndexer;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;

class TestEmitter8 implements Elasticsearch8Emitter<Tuple2<Integer, RequestTest.Product>> {

    private final String index;

    public static TestEmitter8 jsonEmitter(String index, String dataFieldName) {
        return new TestEmitter8(index);
    }

    private TestEmitter8(String index) {
        this.index = index;
    }

    @Override
    public void emit(
            Tuple2<Integer, RequestTest.Product> element,
            SinkWriter.Context context,
            RequestIndexer indexer) {
        indexer.add(createBulkOperation(element));
    }

    private BulkOperation createBulkOperation(Tuple2<Integer, RequestTest.Product> element) {
        try {
            return BulkOperation.of(
                    b ->
                            b.index(
                                    IndexOperation.of(
                                            f ->
                                                    f.index(index)
                                                            .id(element.f0.toString())
                                                            .document(element.f1))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
