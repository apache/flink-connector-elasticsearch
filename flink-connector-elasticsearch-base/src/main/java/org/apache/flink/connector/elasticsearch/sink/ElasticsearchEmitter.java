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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.connector.sink2.SinkWriter;

import org.elasticsearch.action.ActionRequest;

/**
 * Creates one or multiple {@link ActionRequest ActionRequests} from the incoming elements.
 *
 * <p>This is used by sinks to prepare elements for sending them to Elasticsearch.
 *
 * <p>Example:
 *
 * <pre>{@code
 * private static class TestElasticsearchEmitter implements ElasticsearchEmitter<Tuple2<Integer, String>> {
 *
 *     public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
 *         Map<String, Object> document = new HashMap<>();
 * 		   document.put("data", element.f1);
 *
 * 	       return Requests.indexRequest()
 * 		       .index("my-index")
 * 			   .type("my-type")
 * 			   .id(element.f0.toString())
 * 			   .source(document);
 *     }
 *
 * 	   public void emit(Tuple2<Integer, String> element, RequestIndexer indexer) {
 * 	       indexer.add(createIndexRequest(element));
 *     }
 * }
 *
 * }</pre>
 *
 * @param <T> The type of the element handled by this {@link ElasticsearchEmitter}
 */
@PublicEvolving
public interface ElasticsearchEmitter<T> extends Function {

    /**
     * Initialization method for the function. It is called once before the actual working process
     * methods.
     */
    default void open() throws Exception {}

    /** Tear-down method for the function. It is called when the sink closes. */
    default void close() throws Exception {}

    /**
     * Process the incoming element to produce multiple {@link ActionRequest ActionRequests}. The
     * produced requests should be added to the provided {@link RequestIndexer}.
     *
     * @param element incoming element to process
     * @param context to access additional information about the record
     * @param indexer request indexer that {@code ActionRequest} should be added to
     */
    void emit(T element, SinkWriter.Context context, RequestIndexer indexer);
}
