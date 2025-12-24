package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.table.search.ElasticsearchRowDataVectorSearchFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

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
    public VectorSearchRuntimeProvider getSearchRuntimeProvider(
            VectorSearchContext vectorSearchContext) {

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();

        ElasticsearchRowDataVectorSearchFunction vectorSearchFunction =
                new ElasticsearchRowDataVectorSearchFunction(
                        this.format.createRuntimeDecoder(vectorSearchContext, physicalRowDataType),
                        this.maxRetryTimes,
                        ((Elasticsearch7Configuration) config).getVectorSearchMetric(),
                        config.getIndex(),
                        getSearchColumn(vectorSearchContext),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        config.getHosts(),
                        networkClientConfig,
                        (ElasticsearchApiCallBridge<RestHighLevelClient>) apiCallBridge);

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

        return ((RowType) (physicalRowDataType.getLogicalType()))
                .getFieldNames()
                .get(searchColumnIndex);
    }
}
