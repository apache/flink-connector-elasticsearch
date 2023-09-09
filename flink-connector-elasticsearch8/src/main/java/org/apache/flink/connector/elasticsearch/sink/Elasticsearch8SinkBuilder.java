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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.elasticsearch.bulk.BulkProcessor;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import java.util.concurrent.TimeUnit;

/**
 * Builder to construct an Elasticsearch 8 compatible {@link Elasticsearch8Sink}.
 *
 * <p>The following example shows the minimal setup to create a ElasticsearchSink that submits
 * actions on checkpoint or the default number of actions was buffered (1000).
 *
 * <pre>{@code
 * ElasticsearchSink<String> sink = new Elasticsearch8SinkBuilder<String>()
 *     .setHosts(new HttpHost("localhost:9200")
 *     .setEmitter((element, context, indexer) -> {
 *          indexer.add(
 *              new IndexRequest("my-index")
 *              .id(element.f0.toString())
 *              .source(element.f1)
 *          );
 *      })
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records converted to Elasticsearch actions
 */
@PublicEvolving
public class Elasticsearch8SinkBuilder<IN>
        extends Elasticsearch8SinkBuilderBase<IN, Elasticsearch8SinkBuilder<IN>> {

    public Elasticsearch8SinkBuilder() {}

    @Override
    public <T extends IN> Elasticsearch8SinkBuilder<T> setEmitter(
            Elasticsearch8Emitter<? super T> emitter) {
        super.<T>setEmitter(emitter);
        return self();
    }

    @Override
    protected BulkProcessorFactory<IN> getBulkProcessorBuilderFactory() {
        return new BulkProcessorFactory<IN>() {
            @Override
            public BulkProcessor.Builder<IN> apply(
                    ElasticsearchClient client,
                    BulkProcessorConfig bulkProcessorConfig,
                    BulkProcessor.BulkListener<IN> listener) {
                int maxOperations =
                        (bulkProcessorConfig.getBulkFlushMaxActions() != -1)
                                ? bulkProcessorConfig.getBulkFlushMaxActions()
                                : 1;
                long flushInterval = bulkProcessorConfig.getBulkFlushInterval();
                int maxSize =
                        (bulkProcessorConfig.getBulkFlushMaxMb() != -1)
                                ? bulkProcessorConfig.getBulkFlushMaxMb()
                                : 1;
                BulkProcessor.Builder<IN> builder =
                        BulkProcessor.ofBuilder(
                                b ->
                                        b.client(client)
                                                .listener(listener)
                                                .maxOperations(maxOperations)
                                                .maxSize(maxSize)
                                                .flushInterval(
                                                        flushInterval, TimeUnit.MILLISECONDS));
                return builder;
            }
        };
    }
}
