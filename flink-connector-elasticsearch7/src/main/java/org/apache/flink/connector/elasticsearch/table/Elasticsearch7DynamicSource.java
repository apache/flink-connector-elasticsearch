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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.table.search.ElasticsearchRowDataVectorSearchFunction;
import org.apache.flink.connector.elasticsearch.table.search.VectorSearchUtils;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nullable;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link Elasticsearch7DynamicSource}
 * from a logical description.
 */
public class Elasticsearch7DynamicSource extends ElasticsearchDynamicSource
        implements VectorSearchTableSource {

    public Elasticsearch7DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            ElasticsearchConfiguration config,
            DataType physicalRowDataType,
            int maxRetryTimes,
            String summaryString,
            ElasticsearchApiCallBridge<RestHighLevelClient> apiCallBridge,
            @Nullable LookupCache lookupCache,
            @Nullable String docType) {
        super(
                format,
                config,
                physicalRowDataType,
                maxRetryTimes,
                summaryString,
                apiCallBridge,
                lookupCache,
                docType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public DynamicTableSource copy() {
        return new Elasticsearch7DynamicSource(
                format,
                config,
                physicalRowDataType,
                maxRetryTimes,
                summaryString,
                (ElasticsearchApiCallBridge<RestHighLevelClient>) apiCallBridge,
                lookupCache,
                docType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public VectorSearchRuntimeProvider getSearchRuntimeProvider(
            VectorSearchContext vectorSearchContext) {

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();

        ElasticsearchRowDataVectorSearchFunction vectorSearchFunction =
                new ElasticsearchRowDataVectorSearchFunction(
                        this.format.createRuntimeDecoder(vectorSearchContext, physicalRowDataType),
                        this.maxRetryTimes,
                        ((Elasticsearch7Configuration) config).getVectorSearchMetric(),
                        config.getIndex(),
                        VectorSearchUtils.resolveSearchColumn(
                                physicalRowDataType, vectorSearchContext),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        config.getHosts(),
                        networkClientConfig,
                        (ElasticsearchApiCallBridge<RestHighLevelClient>) apiCallBridge);

        return VectorSearchFunctionProvider.of(vectorSearchFunction);
    }
}
