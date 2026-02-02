package org.apache.flink.connector.elasticsearch.table.search;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.NetworkClientConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link VectorSearchFunction} implementation for Elasticsearch 7. */
public class ElasticsearchRowDataVectorSearchFunction
        extends AbstractElasticsearchVectorSearchFunction {
    private static final long serialVersionUID = 1L;
    private static final String QUERY_VECTOR = "query_vector";

    private final ElasticsearchApiCallBridge<RestHighLevelClient> callBridge;
    private final NetworkClientConfig networkClientConfig;
    private final List<HttpHost> hosts;
    private final String scriptScore;

    private transient RestHighLevelClient client;
    private transient SearchRequest searchRequest;
    private transient SearchSourceBuilder searchSourceBuilder;

    public ElasticsearchRowDataVectorSearchFunction(
            DeserializationSchema<RowData> deserializationSchema,
            int maxRetryTimes,
            SearchMetric searchMetric,
            String index,
            String searchColumn,
            String[] producedNames,
            List<HttpHost> hosts,
            NetworkClientConfig networkClientConfig,
            ElasticsearchApiCallBridge<RestHighLevelClient> callBridge) {
        super(deserializationSchema, maxRetryTimes, index, searchColumn, producedNames);
        this.networkClientConfig =
                checkNotNull(networkClientConfig, "No networkClientConfig supplied.");
        this.hosts = checkNotNull(hosts, "No hosts supplied.");
        this.callBridge = checkNotNull(callBridge, "No ElasticsearchApiCallBridge supplied.");
        this.scriptScore =
                String.format(
                        "%s(params.%s, '%s') + 1.0",
                        searchMetric.toString(), QUERY_VECTOR, searchColumn);
    }

    @Override
    protected void doOpen(FunctionContext context) {
        this.client = callBridge.createClient(networkClientConfig, hosts);

        // Reuse searchRequest / searchSourceBuilder across invocations to avoid rebuilding them
        // per record.
        this.searchRequest = new SearchRequest(index);
        this.searchSourceBuilder = new SearchSourceBuilder();
        this.searchSourceBuilder.fetchSource(producedNames, null);
    }

    @Override
    protected SearchResult[] doSearch(int topK, RowData features) throws IOException {
        // Elasticsearch 7.x doesn't support ANN, we use script score to achieve exact matching.
        Map<String, Object> params =
                Collections.singletonMap(QUERY_VECTOR, features.getArray(0).toFloatArray());

        Script script = new Script(ScriptType.INLINE, "painless", scriptScore, params);
        ScriptScoreQueryBuilder scriptScoreQuery =
                new ScriptScoreQueryBuilder(new MatchAllQueryBuilder(), script);

        searchSourceBuilder.query(scriptScoreQuery).size(topK);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        return Stream.of(searchHits)
                .filter(hit -> hit.getSourceAsString() != null)
                .map(hit -> new SearchResult(hit.getSourceAsString(), (double) hit.getScore()))
                .toArray(SearchResult[]::new);
    }
}
