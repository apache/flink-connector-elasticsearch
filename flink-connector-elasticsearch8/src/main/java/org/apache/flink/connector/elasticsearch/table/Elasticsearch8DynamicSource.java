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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.sink.NetworkConfig;
import org.apache.flink.connector.elasticsearch.table.search.ElasticsearchRowDataVectorSearchFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.SerializableSupplier;

import co.elastic.clients.transport.TransportUtils;
import org.apache.http.HttpHost;

import javax.net.ssl.SSLContext;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link Elasticsearch8DynamicSource}
 * from a logical description.
 */
public class Elasticsearch8DynamicSource
        implements VectorSearchTableSource, SupportsProjectionPushDown {

    protected final DecodingFormat<DeserializationSchema<RowData>> format;
    protected final Elasticsearch8Configuration config;
    private final String summaryString;
    protected DataType physicalRowDataType;

    public Elasticsearch8DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            Elasticsearch8Configuration config,
            DataType physicalRowDataType,
            String summaryString) {
        this.format = format;
        this.config = config;
        this.physicalRowDataType = physicalRowDataType;
        this.summaryString = summaryString;
    }

    @Override
    public VectorSearchRuntimeProvider getSearchRuntimeProvider(
            VectorSearchContext vectorSearchContext) {

        ElasticsearchRowDataVectorSearchFunction vectorSearchFunction =
                new ElasticsearchRowDataVectorSearchFunction(
                        format.createRuntimeDecoder(vectorSearchContext, physicalRowDataType),
                        config.getMaxRetries(),
                        config.getNumCandidates(),
                        config.getIndex(),
                        getSearchColumn(vectorSearchContext),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        buildNetworkConfig());

        return VectorSearchFunctionProvider.of(vectorSearchFunction);
    }

    private String getSearchColumn(VectorSearchContext vectorSearchContext) {
        int[][] searchColumns = vectorSearchContext.getSearchColumns();

        if (searchColumns.length != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Elasticsearch only supports one search columns now, but input search columns size is %d.",
                            searchColumns.length));
        }
        int[] searchColumn = searchColumns[0];
        if (searchColumn.length != 1) {
            throw new IllegalArgumentException(
                    "Elasticsearch doesn't support to search data using nested columns.");
        }
        int searchColumnIndex = searchColumn[0];

        if (searchColumnIndex < 0
                || searchColumnIndex >= physicalRowDataType.getChildren().size()) {
            throw new ValidationException(
                    String.format(
                            "The specified search column with index %d doesn't exist in schema.",
                            searchColumnIndex));
        }

        DataType searchColumnType = physicalRowDataType.getChildren().get(searchColumnIndex);
        if (!searchColumnType.getLogicalType().is(LogicalTypeRoot.ARRAY)
                || !((ArrayType) searchColumnType.getLogicalType())
                        .getElementType()
                        .is(LogicalTypeRoot.FLOAT)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Elasticsearch only supports search data using float vector now, but input search column type is %s.",
                            searchColumnType));
        }

        return ((RowType) physicalRowDataType.getLogicalType())
                .getFieldNames()
                .get(searchColumnIndex);
    }

    private NetworkConfig buildNetworkConfig() {
        List<HttpHost> hosts = config.getHosts();
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");

        String username =
                config.getUsername()
                        .filter(v -> !StringUtils.isNullOrWhitespaceOnly(v))
                        .orElse(null);
        String password =
                config.getPassword()
                        .filter(v -> !StringUtils.isNullOrWhitespaceOnly(v))
                        .orElse(null);
        String pathPrefix =
                config.getPathPrefix()
                        .filter(v -> !StringUtils.isNullOrWhitespaceOnly(v))
                        .orElse(null);

        SerializableSupplier<SSLContext> sslContextSupplier =
                config.getCertificateFingerprint()
                        .filter(v -> !StringUtils.isNullOrWhitespaceOnly(v))
                        .<SerializableSupplier<SSLContext>>map(
                                fp -> () -> TransportUtils.sslContextFromCaFingerprint(fp))
                        .orElse(null);

        return new NetworkConfig(
                hosts,
                username,
                password,
                null,
                pathPrefix,
                config.getConnectionRequestTimeout().map(d -> (int) d.toMillis()).orElse(null),
                config.getConnectionTimeout().map(d -> (int) d.toMillis()).orElse(null),
                config.getSocketTimeout().map(d -> (int) d.toMillis()).orElse(null),
                sslContextSupplier,
                null);
    }

    @Override
    public DynamicTableSource copy() {
        return new Elasticsearch8DynamicSource(format, config, physicalRowDataType, summaryString);
    }

    @Override
    public String asSummaryString() {
        return summaryString;
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
