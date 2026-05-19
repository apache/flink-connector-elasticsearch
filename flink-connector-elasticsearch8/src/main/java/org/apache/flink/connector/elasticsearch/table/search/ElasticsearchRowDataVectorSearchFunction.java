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

package org.apache.flink.connector.elasticsearch.table.search;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.sink.NetworkConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link VectorSearchFunction} implementation for Elasticsearch 8. */
public class ElasticsearchRowDataVectorSearchFunction
        extends AbstractElasticsearchVectorSearchFunction {
    private static final long serialVersionUID = 1L;

    private final int numCandidates;
    private final NetworkConfig networkConfig;

    private transient ElasticsearchClient client;

    public ElasticsearchRowDataVectorSearchFunction(
            DeserializationSchema<RowData> deserializationSchema,
            int maxRetryTimes,
            int numCandidates,
            String index,
            String searchColumn,
            String[] producedNames,
            NetworkConfig networkConfig) {
        super(deserializationSchema, maxRetryTimes, index, searchColumn, producedNames);
        this.numCandidates = numCandidates;
        this.networkConfig = checkNotNull(networkConfig, "No networkConfig supplied.");
    }

    @Override
    protected void doOpen(FunctionContext context) {
        this.client = networkConfig.createEsSyncClient();
    }

    @Override
    protected SearchResult[] doSearch(int topK, RowData features) throws IOException {
        List<Float> queryVector = new ArrayList<>();
        for (float feature : features.getArray(0).toFloatArray()) {
            queryVector.add(feature);
        }

        SearchRequest request =
                new SearchRequest.Builder()
                        .index(index)
                        .knn(
                                kb ->
                                        kb.field(searchColumn)
                                                .numCandidates(numCandidates)
                                                .queryVector(queryVector)
                                                .k(topK))
                        .source(src -> src.filter(f -> f.includes(Arrays.asList(producedNames))))
                        .build();

        SearchResponse<JsonData> searchResponse = client.search(request, JsonData.class);

        return searchResponse.hits().hits().stream()
                .filter(hit -> hit.source() != null)
                .map(hit -> new SearchResult(hit.source().toJson().toString(), hit.score()))
                .toArray(SearchResult[]::new);
    }
}
