/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch7.Elasticsearch7ApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nullable;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link Elasticsearch7DynamicSource}
 * from a logical description.
 */
@Internal
public class Elasticsearch7DynamicSource implements LookupTableSource, SupportsProjectionPushDown {

    private final DecodingFormat<DeserializationSchema<RowData>> format;
    private final Elasticsearch7Configuration config;
    private final int lookupMaxRetryTimes;
    private final LookupCache lookupCache;
    private DataType physicalRowDataType;

    public Elasticsearch7DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            Elasticsearch7Configuration config,
            DataType physicalRowDataType,
            int lookupMaxRetryTimes,
            @Nullable LookupCache lookupCache) {
        this.format = format;
        this.config = config;
        this.physicalRowDataType = physicalRowDataType;
        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.lookupCache = lookupCache;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        RestClientFactory restClientFactory = null;
        if (config.getUsername().isPresent()
                && config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
            restClientFactory =
                    new Elasticsearch7DynamicSink.AuthRestClientFactory(
                            config.getPathPrefix().orElse(null),
                            config.getUsername().get(),
                            config.getPassword().get(), config.getSkipVerifySsl().get());
        } else {
            restClientFactory =
                    new Elasticsearch7DynamicSink.DefaultRestClientFactory(
                            config.getPathPrefix().orElse(null), config.getSkipVerifySsl().get());
        }

        Elasticsearch7ApiCallBridge elasticsearch7ApiCallBridge =
                new Elasticsearch7ApiCallBridge(config.getHosts(), restClientFactory);

        // Elasticsearch only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Elasticsearch only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }

        ElasticsearchRowDataLookupFunction<RestHighLevelClient> lookupFunction =
                new ElasticsearchRowDataLookupFunction<>(
                        this.format.createRuntimeDecoder(context, physicalRowDataType),
                        lookupMaxRetryTimes,
                        config.getIndex(),
                        config.getDocumentType(),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        elasticsearch7ApiCallBridge);
        if (lookupCache != null) {
            return PartialCachingLookupProvider.of(lookupFunction, lookupCache);
        } else {
            return LookupFunctionProvider.of(lookupFunction);
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new Elasticsearch7DynamicSource(
                format, config, physicalRowDataType, lookupMaxRetryTimes, lookupCache);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch7";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType type) {
        this.physicalRowDataType = Projection.of(projectedFields).project(type);
    }
}
