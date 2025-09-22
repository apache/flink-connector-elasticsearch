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
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.Operation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import org.apache.http.HttpHost;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** ElasticSearch backed {@link AsyncDynamicTableSink}. */
@Internal
public class ElasticSearch8AsyncDynamicSink extends AsyncDynamicTableSink<Operation> {
    final transient EncodingFormat<SerializationSchema<RowData>> format;
    final DataType physicalRowDataType;
    final List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
    final Elasticsearch8Configuration config;
    final ZoneId localTimeZoneId;

    final String summaryString;
    final boolean isDynamicIndexWithSystemTime;

    public ElasticSearch8AsyncDynamicSink(
            EncodingFormat<SerializationSchema<RowData>> format,
            Elasticsearch8Configuration config,
            List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex,
            DataType physicalRowDataType,
            String summaryString,
            ZoneId localTimeZoneId) {
        super(
                config.getBulkFlushMaxActions(),
                config.getBulkFlushMaxInFlightActions(),
                config.getBulkFlushMaxBufferedActions(),
                config.getBulkFlushMaxByteSize().getBytes(),
                config.getBulkFlushInterval());
        this.format = checkNotNull(format);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyLogicalTypesWithIndex = checkNotNull(primaryKeyLogicalTypesWithIndex);
        this.config = checkNotNull(config);
        this.summaryString = checkNotNull(summaryString);
        this.localTimeZoneId = localTimeZoneId;
        this.isDynamicIndexWithSystemTime = isDynamicIndexWithSystemTime();
    }

    public boolean isDynamicIndexWithSystemTime() {
        IndexGeneratorFactory.IndexHelper indexHelper = new IndexGeneratorFactory.IndexHelper();
        return indexHelper.checkIsDynamicIndexWithSystemTimeFormat(config.getIndex());
    }

    Function<RowData, String> createKeyExtractor() {
        return KeyExtractor.createKeyExtractor(
                primaryKeyLogicalTypesWithIndex, config.getKeyDelimiter());
    }

    IndexGenerator createIndexGenerator() {
        return IndexGeneratorFactory.createIndexGenerator(
                config.getIndex(),
                DataType.getFieldNames(physicalRowDataType),
                DataType.getFieldDataTypes(physicalRowDataType),
                localTimeZoneId);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        boolean hasUpdateOrDelete = false;
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
                if (kind == RowKind.UPDATE_AFTER || kind == RowKind.DELETE) {
                    hasUpdateOrDelete = true;
                }
            }
        }
        if (isDynamicIndexWithSystemTime && !requestedMode.containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    "Dynamic indexing based on system time only works on append only stream.");
        }
        if (hasUpdateOrDelete && primaryKeyLogicalTypesWithIndex.isEmpty()) {
            throw new ValidationException(
                    "Primary key is required when the changelog contains UPDATE or DELETE operations.");
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Elasticsearch8AsyncSinkBuilder<RowData> builder = new Elasticsearch8AsyncSinkBuilder<>();

        builder.setHosts(config.getHosts().toArray(new HttpHost[0]));
        builder.setMaxBatchSize(config.getBulkFlushMaxActions());
        builder.setMaxBufferedRequests(config.getBulkFlushMaxBufferedActions());
        builder.setMaxBatchSizeInBytes(config.getBulkFlushMaxByteSize().getBytes());
        builder.setMaxTimeInBufferMS(config.getBulkFlushInterval());
        builder.setElementConverter(
                new RowDataElementConverter(
                        physicalRowDataType, createIndexGenerator(), createKeyExtractor()));

        config.getUsername()
                .filter(username -> !StringUtils.isNullOrWhitespaceOnly(username))
                .ifPresent(builder::setUsername);

        config.getPassword()
                .filter(password -> !StringUtils.isNullOrWhitespaceOnly(password))
                .ifPresent(builder::setPassword);

        config.getPathPrefix()
                .filter(pathPrefix -> !StringUtils.isNullOrWhitespaceOnly(pathPrefix))
                .ifPresent(builder::setConnectionPathPrefix);

        config.getConnectionRequestTimeout()
                .map(timeout -> (int) timeout.toMillis())
                .ifPresent(builder::setConnectionRequestTimeout);

        config.getConnectionTimeout()
                .map(timeout -> (int) timeout.toMillis())
                .ifPresent(builder::setConnectionTimeout);

        config.getSocketTimeout()
                .map(timeout -> (int) timeout.toMillis())
                .ifPresent(builder::setSocketTimeout);

        config.getCertificateFingerprint().ifPresent(builder::setCertificateFingerprint);

        return SinkV2Provider.of(builder.build(), config.getParallelism().orElse(null));
    }

    @Override
    public DynamicTableSink copy() {
        return new ElasticSearch8AsyncDynamicSink(
                format,
                config,
                primaryKeyLogicalTypesWithIndex,
                physicalRowDataType,
                summaryString,
                localTimeZoneId);
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ElasticSearch8AsyncDynamicSink that = (ElasticSearch8AsyncDynamicSink) o;
        return Objects.equals(format, that.format)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(
                        primaryKeyLogicalTypesWithIndex, that.primaryKeyLogicalTypesWithIndex)
                && Objects.equals(config, that.config)
                && Objects.equals(summaryString, that.summaryString)
                && Objects.equals(localTimeZoneId, that.localTimeZoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                format,
                physicalRowDataType,
                primaryKeyLogicalTypesWithIndex,
                config,
                summaryString,
                localTimeZoneId);
    }

    /** Builder class for {@link ElasticSearch8AsyncDynamicSink}. */
    @Internal
    public static class ElasticSearch8AsyncDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<Operation, ElasticSearch8AsyncDynamicSinkBuilder> {

        DataType physicalRowDataType;
        List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
        ZoneId localTimeZoneId;
        String summaryString;
        Elasticsearch8Configuration config;
        EncodingFormat<SerializationSchema<RowData>> format;

        public ElasticSearch8AsyncDynamicSinkBuilder setConfig(Elasticsearch8Configuration config) {
            this.config = config;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setFormat(
                EncodingFormat<SerializationSchema<RowData>> format) {
            this.format = format;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setPhysicalRowDataType(
                DataType physicalRowDataType) {
            this.physicalRowDataType = physicalRowDataType;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setPrimaryKeyLogicalTypesWithIndex(
                List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex) {
            this.primaryKeyLogicalTypesWithIndex = primaryKeyLogicalTypesWithIndex;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setLocalTimeZoneId(ZoneId localTimeZoneId) {
            this.localTimeZoneId = localTimeZoneId;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setSummaryString(String summaryString) {
            this.summaryString = summaryString;
            return this;
        }

        @Override
        public ElasticSearch8AsyncDynamicSink build() {
            return new ElasticSearch8AsyncDynamicSink(
                    format,
                    config,
                    primaryKeyLogicalTypesWithIndex,
                    physicalRowDataType,
                    summaryString,
                    localTimeZoneId);
        }
    }
}
