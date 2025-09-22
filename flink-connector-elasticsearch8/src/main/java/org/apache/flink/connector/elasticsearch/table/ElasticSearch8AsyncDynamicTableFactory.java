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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.capitalize;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_BUFFERED_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_IN_FLIGHT_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.CONNECTION_PATH_PREFIX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.CONNECTION_REQUEST_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DELIVERY_GUARANTEE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SOCKET_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_CERTIFICATE_FINGERPRINT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.MAX_RETRIES;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Factory for creating {@link ElasticSearch8AsyncDynamicSink} . */
@Internal
public class ElasticSearch8AsyncDynamicTableFactory extends AsyncDynamicTableSinkFactory {

    private static final String IDENTIFIER = "elasticsearch-8";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex =
                getPrimaryKeyLogicalTypesWithIndex(context);

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT_OPTION);

        Elasticsearch8Configuration config = getConfiguration(helper);
        helper.validate();
        validateConfiguration(config);

        ElasticSearch8AsyncDynamicSink.ElasticSearch8AsyncDynamicSinkBuilder builder =
                new ElasticSearch8AsyncDynamicSink.ElasticSearch8AsyncDynamicSinkBuilder();

        return builder.setConfig(config)
                .setFormat(format)
                .setPrimaryKeyLogicalTypesWithIndex(primaryKeyLogicalTypesWithIndex)
                .setPhysicalRowDataType(context.getPhysicalRowDataType())
                .setLocalTimeZoneId(getLocalTimeZoneId(context.getConfiguration()))
                .setSummaryString(capitalize(IDENTIFIER))
                .build();
    }

    ZoneId getLocalTimeZoneId(ReadableConfig readableConfig) {
        final String zone = readableConfig.get(TableConfigOptions.LOCAL_TIME_ZONE);

        return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                ? ZoneId.systemDefault()
                : ZoneId.of(zone);
    }

    List<LogicalTypeWithIndex> getPrimaryKeyLogicalTypesWithIndex(Context context) {
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        int[] primaryKeyIndexes = context.getPrimaryKeyIndexes();
        if (primaryKeyIndexes.length != 0) {
            DataType pkDataType = Projection.of(primaryKeyIndexes).project(physicalRowDataType);

            ElasticsearchValidationUtils.validatePrimaryKey(pkDataType);
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return Arrays.stream(primaryKeyIndexes)
                .mapToObj(
                        index -> {
                            Optional<Column> column = resolvedSchema.getColumn(index);
                            if (!column.isPresent()) {
                                throw new IllegalStateException(
                                        String.format(
                                                "No primary key column found with index '%s'.",
                                                index));
                            }
                            LogicalType logicalType = column.get().getDataType().getLogicalType();
                            return new LogicalTypeWithIndex(index, logicalType);
                        })
                .collect(Collectors.toList());
    }

    Elasticsearch8Configuration getConfiguration(FactoryUtil.TableFactoryHelper helper) {
        return new Elasticsearch8Configuration(helper.getOptions());
    }

    void validateConfiguration(Elasticsearch8Configuration config) {
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
                maxActions >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
        long maxSize = config.getBulkFlushMaxByteSize().getBytes();
        long mb1 = 1024 * 1024;
        validate(
                maxSize >= mb1 && maxSize % mb1 == 0,
                () ->
                        String.format(
                                "'%s' must be in MB granularity. Got: %s",
                                BULK_FLUSH_MAX_SIZE_OPTION.key(),
                                config.getBulkFlushMaxByteSize().toHumanReadableString()));
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

    static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_MAX_BUFFERED_ACTIONS_OPTION,
                        BULK_FLUSH_MAX_IN_FLIGHT_ACTIONS_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        CONNECTION_REQUEST_TIMEOUT,
                        CONNECTION_TIMEOUT,
                        SOCKET_TIMEOUT,
                        SSL_CERTIFICATE_FINGERPRINT,
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
                        MAX_RETRIES)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        HOSTS_OPTION,
                        INDEX_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_MAX_BUFFERED_ACTIONS_OPTION,
                        BULK_FLUSH_MAX_IN_FLIGHT_ACTIONS_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        CONNECTION_REQUEST_TIMEOUT,
                        CONNECTION_TIMEOUT,
                        SOCKET_TIMEOUT,
                        SSL_CERTIFICATE_FINGERPRINT)
                .collect(Collectors.toSet());
    }
}
