package org.apache.flink.connector.elasticsearch.table.search;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.NetworkClientConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;
import org.apache.flink.util.FlinkRuntimeException;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link VectorSearchFunction} implementation for Elasticsearch. */
public class ElasticsearchRowDataVectorSearchFunction extends VectorSearchFunction {
    private static final Logger LOG =
            LoggerFactory.getLogger(ElasticsearchRowDataVectorSearchFunction.class);
    private static final long serialVersionUID = 1L;
    private static final String QUERY_VECTOR = "query_vector";

    private final DeserializationSchema<RowData> deserializationSchema;

    private final String index;

    private final String[] producedNames;
    private final int maxRetryTimes;
    private final SearchMetric searchMetric;
    private SearchRequest searchRequest;
    private SearchSourceBuilder searchSourceBuilder;

    private final ElasticsearchApiCallBridge<RestHighLevelClient> callBridge;
    private final NetworkClientConfig networkClientConfig;
    private final List<HttpHost> hosts;
    private final String scriptScore;

    private transient RestHighLevelClient client;

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

        checkNotNull(deserializationSchema, "No DeserializationSchema supplied.");
        checkNotNull(maxRetryTimes, "No maxRetryTimes supplied.");
        checkNotNull(producedNames, "No fieldNames supplied.");
        checkNotNull(hosts, "No hosts supplied.");
        checkNotNull(networkClientConfig, "No networkClientConfig supplied.");
        checkNotNull(callBridge, "No ElasticsearchApiCallBridge supplied.");

        this.deserializationSchema = deserializationSchema;
        this.maxRetryTimes = maxRetryTimes;
        this.searchMetric = searchMetric;
        this.index = index;
        this.producedNames = producedNames;

        this.networkClientConfig = networkClientConfig;
        this.hosts = hosts;
        this.callBridge = callBridge;
        this.scriptScore =
                String.format(
                        "%s(params.%s, '%s') + 1.0",
                        searchMetric.toString(), QUERY_VECTOR, searchColumn);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.client = callBridge.createClient(networkClientConfig, hosts);

        // Set searchRequest in open method in case of amount of calling in eval method when every
        // record comes.
        this.searchRequest = new SearchRequest(index);
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(producedNames, null);
        deserializationSchema.open(null);
    }

    @Override
    public Collection<RowData> vectorSearch(int topK, RowData features) throws IOException {
        // Elasticsearch 7.x doesn't support ANN, we use script score to achieve exact matching.
        Map<String, Object> params =
                Collections.singletonMap(QUERY_VECTOR, features.getArray(0).toFloatArray());

        Script script = new Script(ScriptType.INLINE, "painless", scriptScore, params);

        ScriptScoreQueryBuilder scriptScoreQuery =
                new ScriptScoreQueryBuilder(new MatchAllQueryBuilder(), script);

        searchSourceBuilder.query(scriptScoreQuery).size(topK);

        searchRequest.source(searchSourceBuilder);

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                ArrayList<RowData> rows = new ArrayList<>();
                Tuple2<String, SearchResult[]> searchResponse = search(client, searchRequest);

                if (searchResponse.f1.length > 0) {
                    for (SearchResult result : searchResponse.f1) {
                        String source = result.source;
                        RowData row = parseSearchResult(source);
                        GenericRowData scoreData = new GenericRowData(1);
                        scoreData.setField(0, Double.valueOf(result.score));
                        if (row != null) {
                            rows.add(new JoinedRowData(row, scoreData));
                        }
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (IOException e) {
                LOG.error(String.format("Elasticsearch search error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new FlinkRuntimeException("Execution of Elasticsearch search failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    LOG.warn(
                            "Interrupted while waiting to retry failed elasticsearch search, aborting");
                    throw new FlinkRuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    private RowData parseSearchResult(String result) {
        RowData row = null;
        try {
            row = deserializationSchema.deserialize(result.getBytes());
        } catch (IOException e) {
            LOG.error("Deserialize search hit failed: " + e.getMessage());
        }

        return row;
    }

    private Tuple2<String, SearchResult[]> search(
            RestHighLevelClient client, SearchRequest searchRequest) throws IOException {
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        return new Tuple2<>(
                searchResponse.getScrollId(),
                Stream.of(searchHits)
                        .map(hit -> new SearchResult(hit.getSourceAsString(), hit.getScore()))
                        .toArray(SearchResult[]::new));
    }

    private static class SearchResult {
        private final String source;
        private final Float score;

        public SearchResult(String source, Float score) {
            this.source = source;
            this.score = score;
        }
    }
}
