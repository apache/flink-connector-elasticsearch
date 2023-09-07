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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.INDEX_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.MAX_RETRIES;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;

/**
 * A {@link DynamicTableFactory} for discovering {@link Elasticsearch7DynamicSource} and {@link
 * Elasticsearch7DynamicSink}.
 */
@Internal
public class Elasticsearch7DynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private static final Set<ConfigOption<?>> requiredOptions =
            Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    private static final Set<ConfigOption<?>> optionalOptions =
            Stream.of(
                            KEY_DELIMITER_OPTION,
                            FAILURE_HANDLER_OPTION,
                            FLUSH_ON_CHECKPOINT_OPTION,
                            BULK_FLASH_MAX_SIZE_OPTION,
                            BULK_FLUSH_MAX_ACTIONS_OPTION,
                            BULK_FLUSH_INTERVAL_OPTION,
                            BULK_FLUSH_BACKOFF_TYPE_OPTION,
                            BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                            BULK_FLUSH_BACKOFF_DELAY_OPTION,
                            CONNECTION_PATH_PREFIX,
                            FORMAT_OPTION,
                            PASSWORD_OPTION,
                            USERNAME_OPTION,
                            CACHE_TYPE,
                            PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                            PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                            PARTIAL_CACHE_MAX_ROWS,
                            PARTIAL_CACHE_CACHE_MISSING_KEY,
                            MAX_RETRIES)
                    .collect(Collectors.toSet());

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig options = helper.getOptions();
        final DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class,
                        org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions
                                .FORMAT_OPTION);

        helper.validate();

        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);
        Elasticsearch7Configuration config =
                new Elasticsearch7Configuration(configuration, context.getClassLoader());

        return new Elasticsearch7DynamicSource(
                format,
                config,
                physicalRowDataType,
                options.get(MAX_RETRIES),
                getLookupCache(options));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        ElasticsearchValidationUtils.validatePrimaryKey(tableSchema);

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT_OPTION);

        helper.validate();
        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);
        Elasticsearch7Configuration config =
                new Elasticsearch7Configuration(configuration, context.getClassLoader());

        validate(config, configuration);

        return new Elasticsearch7DynamicSink(
                format,
                config,
                TableSchemaUtils.getPhysicalSchema(tableSchema),
                getLocalTimeZoneId(context.getConfiguration()));
    }

    @Nullable
    private LookupCache getLookupCache(ReadableConfig tableOptions) {
        LookupCache cache = null;
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }
        return cache;
    }

    ZoneId getLocalTimeZoneId(ReadableConfig readableConfig) {
        final String zone = readableConfig.get(TableConfigOptions.LOCAL_TIME_ZONE);
        final ZoneId zoneId =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);

        return zoneId;
    }

    private void validate(Elasticsearch7Configuration config, Configuration originalConfiguration) {
        config.getFailureHandler(); // checks if we can instantiate the custom failure handler
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
                maxActions == -1 || maxActions >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
        long maxSize = config.getBulkFlushMaxByteSize();
        long mb1 = 1024 * 1024;
        validate(
                maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
                () ->
                        String.format(
                                "'%s' must be in MB granularity. Got: %s",
                                BULK_FLASH_MAX_SIZE_OPTION.key(),
                                originalConfiguration
                                        .get(BULK_FLASH_MAX_SIZE_OPTION)
                                        .toHumanReadableString()));
        validate(
                config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
                                config.getBulkFlushBackoffRetries().get()));
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            validate(
                    config.getPassword().isPresent()
                            && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
                    () ->
                            String.format(
                                    "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                                    USERNAME_OPTION.key(),
                                    PASSWORD_OPTION.key(),
                                    config.getUsername().get(),
                                    config.getPassword().orElse("")));
        }
    }

    private static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    @Override
    public String factoryIdentifier() {
        return "elasticsearch-7";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return optionalOptions;
    }
}
