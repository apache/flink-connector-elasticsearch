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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.elasticsearch.Elasticsearch7ApiCallBridge;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.elasticsearch.client.RestHighLevelClient;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7ConnectorOptions.MAX_RETRIES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7ConnectorOptions.VECTOR_SEARCH_METRIC;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_REQUEST_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.DELIVERY_GUARANTEE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.SOCKET_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.elasticsearch.common.Strings.capitalize;

/** A {@link DynamicTableSinkFactory} for discovering {@link ElasticsearchDynamicSink}. */
@Internal
public class Elasticsearch7DynamicTableFactory extends ElasticsearchDynamicTableFactoryBase {
    private static final String FACTORY_IDENTIFIER = "elasticsearch-7";

    public Elasticsearch7DynamicTableFactory() {
        super(FACTORY_IDENTIFIER, Elasticsearch7SinkBuilder::new);
    }

    @Override
    ElasticsearchConfiguration getConfiguration(FactoryUtil.TableFactoryHelper helper) {
        return new Elasticsearch7Configuration(helper.getOptions());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig options = helper.getOptions();
        final DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class,
                        org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions
                                .FORMAT_OPTION);

        Elasticsearch7Configuration config = (Elasticsearch7Configuration) getConfiguration(helper);
        helper.validate();
        validateConfiguration(config);

        return new Elasticsearch7DynamicSource(
                format,
                config,
                context.getPhysicalRowDataType(),
                config.getMaxRetries(),
                capitalize(FACTORY_IDENTIFIER),
                getElasticsearchApiCallBridge(),
                getLookupCache(options),
                getDocumentType(config));
    }

    @Override
    ElasticsearchApiCallBridge<RestHighLevelClient> getElasticsearchApiCallBridge() {
        return new Elasticsearch7ApiCallBridge();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_BACKOFF_TYPE_OPTION,
                        BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                        BULK_FLUSH_BACKOFF_DELAY_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        CONNECTION_REQUEST_TIMEOUT,
                        CONNECTION_TIMEOUT,
                        SOCKET_TIMEOUT,
                        FORMAT_OPTION,
                        DELIVERY_GUARANTEE_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        SINK_PARALLELISM,
                        CACHE_TYPE,
                        PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                        PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                        PARTIAL_CACHE_MAX_ROWS,
                        PARTIAL_CACHE_CACHE_MISSING_KEY,
                        MAX_RETRIES,
                        VECTOR_SEARCH_METRIC)
                .collect(Collectors.toSet());
    }
}
