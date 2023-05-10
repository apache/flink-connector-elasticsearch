package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch7.Elasticsearch7ApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

@Internal
public class Elasticsearch7DynamicSource implements ScanTableSource, LookupTableSource,
        SupportsProjectionPushDown {

    private final DecodingFormat<DeserializationSchema<RowData>> format;
    private final Elasticsearch7Configuration config;
    private final ElasticsearchLookupOptions lookupOptions;
    private TableSchema physicalSchema;

    public Elasticsearch7DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            Elasticsearch7Configuration config,
            TableSchema physicalSchema,
            ElasticsearchLookupOptions lookupOptions) {
        this.format = format;
        this.config = config;
        this.physicalSchema = physicalSchema;
        this.lookupOptions = lookupOptions;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        RestClientFactory restClientFactory = null;
        if (config.getPathPrefix().isPresent()) {
            restClientFactory = new Elasticsearch7DynamicSink.DefaultRestClientFactory(config.getPathPrefix().get());
        } else {
            restClientFactory = restClientBuilder -> {};
        }

        Elasticsearch7ApiCallBridge elasticsearch7ApiCallBridge = new Elasticsearch7ApiCallBridge(
                config.getHosts(), restClientFactory);

        // Elasticsearch only support non-nested look up keys
        String[] lookupKeys = new String[lookupContext.getKeys().length];
        String [] columnNames = physicalSchema.getFieldNames();
        for (int i = 0; i < lookupKeys.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1, "Elasticsearch only support non-nested look up keys");
            lookupKeys[i] = columnNames[innerKeyArr[0]];
        }
        DataType[] columnDataTypes = physicalSchema.getFieldDataTypes();

        return TableFunctionProvider.of(new ElasticsearchRowDataLookupFunction(
                this.format.createRuntimeDecoder(lookupContext, physicalSchema.toRowDataType()),
                        lookupOptions,
                        config.getIndex(),
                        config.getDocumentType(),
                        columnNames,
                        columnDataTypes,
                        lookupKeys,
                        elasticsearch7ApiCallBridge
        ));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return null;
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
}
