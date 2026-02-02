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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base {@link VectorSearchFunction} implementation for Elasticsearch. Shared retry loop, result
 * decoding and null-source filtering live here; version-specific subclasses only need to provide
 * the client initialization and the search call.
 */
public abstract class AbstractElasticsearchVectorSearchFunction extends VectorSearchFunction {
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractElasticsearchVectorSearchFunction.class);
    private static final long serialVersionUID = 1L;

    protected final DeserializationSchema<RowData> deserializationSchema;
    protected final String index;
    protected final String searchColumn;
    protected final String[] producedNames;
    protected final int maxRetryTimes;

    protected AbstractElasticsearchVectorSearchFunction(
            DeserializationSchema<RowData> deserializationSchema,
            int maxRetryTimes,
            String index,
            String searchColumn,
            String[] producedNames) {
        this.deserializationSchema =
                checkNotNull(deserializationSchema, "No DeserializationSchema supplied.");
        this.producedNames = checkNotNull(producedNames, "No fieldNames supplied.");
        this.maxRetryTimes = maxRetryTimes;
        this.index = index;
        this.searchColumn = searchColumn;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        doOpen(context);
        deserializationSchema.open(null);
    }

    @Override
    public Collection<RowData> vectorSearch(int topK, RowData features) throws IOException {
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                SearchResult[] results = doSearch(topK, features);
                if (results.length > 0) {
                    ArrayList<RowData> rows = new ArrayList<>(results.length);
                    for (SearchResult result : results) {
                        if (result.source == null) {
                            continue;
                        }
                        RowData row = parseSearchResult(result.source);
                        if (row == null) {
                            continue;
                        }
                        GenericRowData scoreData = new GenericRowData(1);
                        scoreData.setField(0, result.score);
                        rows.add(new JoinedRowData(row, scoreData));
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (IOException e) {
                LOG.error(String.format("Elasticsearch search error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new FlinkRuntimeException("Execution of Elasticsearch search failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    LOG.warn(
                            "Interrupted while waiting to retry failed elasticsearch search, aborting");
                    throw new FlinkRuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    /** Version-specific initialization (e.g., creating the underlying Elasticsearch client). */
    protected abstract void doOpen(FunctionContext context) throws Exception;

    /** Execute a single vector search call and return raw results, excluding nothing. */
    protected abstract SearchResult[] doSearch(int topK, RowData features) throws IOException;

    private RowData parseSearchResult(String result) {
        try {
            return deserializationSchema.deserialize(result.getBytes());
        } catch (IOException e) {
            LOG.error("Deserialize search hit failed: " + e.getMessage());
            return null;
        }
    }

    /** One hit from Elasticsearch — raw JSON source plus score. */
    protected static class SearchResult {
        final String source;
        final Double score;

        public SearchResult(String source, Double score) {
            this.source = source;
            this.score = score;
        }
    }
}
