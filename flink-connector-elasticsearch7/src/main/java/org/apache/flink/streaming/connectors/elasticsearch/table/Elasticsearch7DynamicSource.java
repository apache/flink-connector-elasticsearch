package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticSearch7InputFormat;
import org.apache.flink.streaming.connectors.elasticsearch7.Elasticsearch7ApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.utils.TableSchemaUtils.containsPhysicalColumnsOnly;
import static org.apache.flink.util.Preconditions.checkArgument;

@Internal
public class Elasticsearch7DynamicSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown {

    private final DecodingFormat<DeserializationSchema<RowData>> format;
    private final Elasticsearch7Configuration config;
    private final ElasticsearchLookupOptions lookupOptions;
    private TableSchema physicalSchema;
    private Set<String> filterableFields;
    private Long limit;
    private List<ResolvedExpression> filterPredicates;

    public Elasticsearch7DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            Elasticsearch7Configuration config,
            TableSchema physicalSchema,
            ElasticsearchLookupOptions lookupOptions) {
        this.format = format;
        this.config = config;
        this.physicalSchema = physicalSchema;
        this.lookupOptions = lookupOptions;
        List<String> fieldNameList =  Arrays.asList(physicalSchema.getFieldNames());
        this.filterableFields =  new HashSet<String>(fieldNameList);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        RestClientFactory restClientFactory = null;
        if (config.getPathPrefix().isPresent()) {
            restClientFactory =
                    new Elasticsearch7DynamicSink.DefaultRestClientFactory(
                            config.getPathPrefix().get());
        } else {
            restClientFactory = restClientBuilder -> {};
        }

        Elasticsearch7ApiCallBridge elasticsearch7ApiCallBridge =
                new Elasticsearch7ApiCallBridge(config.getHosts(), restClientFactory);

        // Elasticsearch only support non-nested look up keys
        String[] lookupKeys = new String[lookupContext.getKeys().length];
        String[] columnNames = physicalSchema.getFieldNames();
        for (int i = 0; i < lookupKeys.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Elasticsearch only support non-nested look up keys");
            lookupKeys[i] = columnNames[innerKeyArr[0]];
        }
        DataType[] columnDataTypes = physicalSchema.getFieldDataTypes();

        return TableFunctionProvider.of(
                new ElasticsearchRowDataLookupFunction(
                        this.format.createRuntimeDecoder(
                                lookupContext, physicalSchema.toRowDataType()),
                        lookupOptions,
                        config.getIndex(),
                        config.getDocumentType(),
                        columnNames,
                        columnDataTypes,
                        lookupKeys,
                        elasticsearch7ApiCallBridge));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        ElasticSearch7InputFormat.Builder elasticsearchInputformatBuilder = new ElasticSearch7InputFormat.Builder();
        elasticsearchInputformatBuilder.setHttpHosts(config.getHosts());

        RestClientFactory restClientFactory = null;
        if (config.getPathPrefix().isPresent()) {
            restClientFactory = new Elasticsearch7DynamicSink.DefaultRestClientFactory(config.getPathPrefix().get());
        } else {
            restClientFactory = restClientBuilder -> { };
        }

        elasticsearchInputformatBuilder.setRestClientFactory(restClientFactory);
        elasticsearchInputformatBuilder.setDeserializationSchema(this.format.createRuntimeDecoder(runtimeProviderContext, physicalSchema.toRowDataType()));
        elasticsearchInputformatBuilder.setFieldNames(physicalSchema.getFieldNames());
        elasticsearchInputformatBuilder.setIndex(config.getIndex());
        elasticsearchInputformatBuilder.setPredicate(assembleQuery(filterPredicates));
        elasticsearchInputformatBuilder.setLimit(limit.intValue());
        config.getScrollMaxSize().ifPresent(elasticsearchInputformatBuilder::setScrollMaxSize);
        config.getScrollTimeout().ifPresent(elasticsearchInputformatBuilder::setScrollTimeout);


        return InputFormatProvider.of(
                elasticsearchInputformatBuilder.build()
        );
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = projectSchema(physicalSchema, projectedFields);
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        for (ResolvedExpression expr : filters) {
            if (FilterUtils.shouldPushDown(expr, filterableFields)) {
                acceptedFilters.add(expr);
            } else {
                remainingFilters.add(expr);
            }
        }
        this.filterPredicates = acceptedFilters;
        return Result.of(acceptedFilters, remainingFilters);
    }


    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    public static TableSchema projectSchema(TableSchema tableSchema, int[][] projectedFields) {
        checkArgument(
                containsPhysicalColumnsOnly(tableSchema),
                "Projection is only supported for physical columns.");
        TableSchema.Builder builder = TableSchema.builder();

        FieldsDataType fields =
                (FieldsDataType)
                        DataTypeUtils.projectRow(tableSchema.toRowDataType(), projectedFields);
        RowType topFields = (RowType) fields.getLogicalType();
        for (int i = 0; i < topFields.getFieldCount(); i++) {
            builder.field(topFields.getFieldNames().get(i), fields.getChildren().get(i));
        }
        return builder.build();
    }

    public static QueryBuilder assembleQuery(List<ResolvedExpression> filterPredicates) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (ResolvedExpression resolvedExpression : filterPredicates) {

            if (!(resolvedExpression instanceof CallExpression)) {
                continue;
            }
            CallExpression callExpression = (CallExpression) resolvedExpression;
            FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
            ResolvedExpression valueLiteralExpression = Optional.of(callExpression.getResolvedChildren()).get().get(0);
            ResolvedExpression fieldReferenceExpression = Optional.of(callExpression.getResolvedChildren()).get().get(1);
            ValueLiteralExpression value = (ValueLiteralExpression) valueLiteralExpression;
            FieldReferenceExpression field = (FieldReferenceExpression) fieldReferenceExpression;
            if (functionDefinition.equals(BuiltInFunctionDefinitions.AND)) {
               boolQueryBuilder = boolQueryBuilder.must(QueryBuilders.wildcardQuery(field.getName(),  value.asSummaryString()));
            }

            if (functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
               boolQueryBuilder = boolQueryBuilder.should(QueryBuilders.wildcardQuery(field.getName(),  value.asSummaryString()));
            }

            if (functionDefinition.equals(BuiltInFunctionDefinitions.NOT)) {
                boolQueryBuilder = boolQueryBuilder.mustNot(QueryBuilders.wildcardQuery(field.getName(),  value.asSummaryString()));
            }

            if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field.getName())
                        .gte(value.asSummaryString());
                boolQueryBuilder = boolQueryBuilder.must(rangeQueryBuilder);
            }

            if (functionDefinition.equals(functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN))) {
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field.getName())
                        .gte( value.asSummaryString());
                boolQueryBuilder = boolQueryBuilder.must(rangeQueryBuilder);
            }

            if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
                TermQueryBuilder termQueryBuilder =  QueryBuilders.termQuery(field.getName(),  value.asSummaryString());
                boolQueryBuilder = boolQueryBuilder.must(termQueryBuilder);
            }

            if (functionDefinition.equals(BuiltInFunctionDefinitions.IF_NULL)) {
                ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(field.getName());
                boolQueryBuilder = boolQueryBuilder.must(existsQueryBuilder);
            }

            if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
                ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(field.getName());
                boolQueryBuilder = boolQueryBuilder.must(existsQueryBuilder);
            }
        }
        return boolQueryBuilder;
    }
}
