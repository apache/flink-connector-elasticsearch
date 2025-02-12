package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.lookup.ElasticsearchRowDataLookupFunction;
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

import javax.annotation.Nullable;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link ElasticsearchDynamicSource}
 * from a logical description.
 */
public class ElasticsearchDynamicSource implements LookupTableSource, SupportsProjectionPushDown {
    private final DecodingFormat<DeserializationSchema<RowData>> format;
    private final ElasticsearchConfiguration config;
    private final int lookupMaxRetryTimes;
    private final LookupCache lookupCache;
    private final String docType;
    private final String summaryString;
    private final ElasticsearchApiCallBridge<?> apiCallBridge;
    private DataType physicalRowDataType;

    public ElasticsearchDynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            ElasticsearchConfiguration config,
            DataType physicalRowDataType,
            int lookupMaxRetryTimes,
            String summaryString,
            ElasticsearchApiCallBridge<?> apiCallBridge,
            @Nullable LookupCache lookupCache,
            @Nullable String docType) {
        this.format = format;
        this.config = config;
        this.physicalRowDataType = physicalRowDataType;
        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.summaryString = summaryString;
        this.apiCallBridge = apiCallBridge;
        this.lookupCache = lookupCache;
        this.docType = docType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // Elasticsearch only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Elasticsearch only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
        }

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();

        ElasticsearchRowDataLookupFunction<?> lookupFunction =
                new ElasticsearchRowDataLookupFunction<>(
                        this.format.createRuntimeDecoder(context, physicalRowDataType),
                        lookupMaxRetryTimes,
                        config.getIndex(),
                        docType,
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        keyNames,
                        config.getHosts(),
                        networkClientConfig,
                        apiCallBridge);
        if (lookupCache != null) {
            return PartialCachingLookupProvider.of(lookupFunction, lookupCache);
        } else {
            return LookupFunctionProvider.of(lookupFunction);
        }
    }

    private NetworkClientConfig buildNetworkClientConfig() {
        NetworkClientConfig.Builder builder = new NetworkClientConfig.Builder();
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            builder.setUsername(config.getUsername().get());
        }

        if (config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
            builder.setPassword(config.getPassword().get());
        }

        if (config.getPathPrefix().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPathPrefix().get())) {
            builder.setConnectionPathPrefix(config.getPathPrefix().get());
        }

        if (config.getConnectionRequestTimeout().isPresent()) {
            builder.setConnectionRequestTimeout(
                    (int) config.getConnectionRequestTimeout().get().getSeconds());
        }

        if (config.getConnectionTimeout().isPresent()) {
            builder.setConnectionTimeout((int) config.getConnectionTimeout().get().getSeconds());
        }

        if (config.getSocketTimeout().isPresent()) {
            builder.setSocketTimeout((int) config.getSocketTimeout().get().getSeconds());
        }

        return builder.build();
    }

    @Override
    public DynamicTableSource copy() {
        return new ElasticsearchDynamicSource(
                format,
                config,
                physicalRowDataType,
                lookupMaxRetryTimes,
                summaryString,
                apiCallBridge,
                lookupCache,
                docType);
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
