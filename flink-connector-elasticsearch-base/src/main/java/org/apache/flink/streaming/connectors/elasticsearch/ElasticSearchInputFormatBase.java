package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.curator5.com.google.common.base.Preconditions.checkNotNull;

@Internal
public class ElasticSearchInputFormatBase <T, C extends AutoCloseable> extends RichInputFormat<T, ElasticsearchInputSplit>
        implements ResultTypeQueryable<T> {


    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchInputFormatBase.class);
    private static final long serialVersionUID = 1L;
    private final DeserializationSchema<T> deserializationSchema;
    private final String index;
    private final String type;

    private final String[] fieldNames;
    private final QueryBuilder predicate;
    private final long limit;

    private final long scrollTimeout;
    private final int scrollSize;

    private Scroll scroll;
    private String currentScrollWindowId;
    private String[] currentScrollWindowHits;

    private int nextRecordIndex = 0;
    private long currentReadCount = 0L;

    /**
     * Call bridge for different version-specific.
     */
    private final ElasticsearchApiCallBridge<C> callBridge;

    private final Map<String, String> userConfig;
    /**
     * Elasticsearch client created using the call bridge.
     */
    private transient C client;

    public ElasticSearchInputFormatBase(
            ElasticsearchApiCallBridge<C> callBridge,
            Map<String, String> userConfig,
            DeserializationSchema<T> deserializationSchema,
            String[] fieldNames,
            String index,
            String type,
            long scrollTimeout,
            int scrollSize,
            QueryBuilder predicate,
            long limit) {

        this.callBridge = checkNotNull(callBridge);
        checkNotNull(userConfig);
        // copy config so we can remove entries without side-effects
        this.userConfig = new HashMap<>(userConfig);

        this.deserializationSchema = checkNotNull(deserializationSchema);
        this.index = index;
        this.type = type;

        this.fieldNames = fieldNames;
        this.predicate = predicate;
        this.limit = limit;

        this.scrollTimeout = scrollTimeout;
        this.scrollSize = scrollSize;

    }
    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public void configure(Configuration configuration) {

        try {
            client = callBridge.createClient();
            callBridge.verifyClientConnection(client);
        } catch (IOException ex) {
            LOG.error("Exception while creating connection to Elasticsearch.", ex);
            throw new RuntimeException("Cannot create connection to Elasticsearcg.", ex);
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public ElasticsearchInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return callBridge.createInputSplitsInternal(client, index, type, minNumSplits);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(ElasticsearchInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(ElasticsearchInputSplit split) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        if (type == null) {
            searchRequest.types(Strings.EMPTY_ARRAY);
        } else {
            searchRequest.types(type);
        }
        this.scroll = new Scroll(TimeValue.timeValueMinutes(scrollTimeout));
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        int size;
        if (limit > 0) {
            size = (int) Math.min(limit, scrollSize);
        } else {
            size = scrollSize;
        }
        //elasticsearch default value is 10 here.
        searchSourceBuilder.size(size);
        searchSourceBuilder.fetchSource(fieldNames, null);
        if (predicate != null) {
            searchSourceBuilder.query(predicate);
        } else {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }
        searchRequest.source(searchSourceBuilder);
        searchRequest.preference("_shards:" + split.getShard());
        Tuple2<String, String[]> searchResponse = null;
        try {

            searchResponse = callBridge.search(client, searchRequest);
        } catch (IOException e) {
            LOG.error("Search has error: ", e.getMessage());
        }
        if (searchResponse != null) {
            currentScrollWindowId = searchResponse.f0;
            currentScrollWindowHits = searchResponse.f1;
            nextRecordIndex = 0;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (limit > 0 && currentReadCount >= limit) {
            return true;
        }

        // SearchResponse can be InternalSearchHits.empty(), and the InternalSearchHit[] EMPTY = new InternalSearchHit[0]
        if (currentScrollWindowHits.length != 0 && nextRecordIndex > currentScrollWindowHits.length - 1) {
            fetchNextScrollWindow();
        }

        return currentScrollWindowHits.length == 0;
    }

    @Override
    public T nextRecord(T t) throws IOException {
        if (reachedEnd()) {
            LOG.warn("Already reached the end of the split.");
        }

        String hit = currentScrollWindowHits[nextRecordIndex];
        nextRecordIndex++;
        currentReadCount++;
        LOG.debug("Yielding new record for hit: " + hit);

        return parseSearchHit(hit);
    }

    @Override
    public void close() throws IOException {
        callBridge.close(client);
    }

    private void fetchNextScrollWindow() {
        Tuple2<String, String[]> searchResponse = null;
        SearchScrollRequest scrollRequest = new SearchScrollRequest(currentScrollWindowId);
        scrollRequest.scroll(scroll);

        try {
            searchResponse = callBridge.scroll(client, scrollRequest);
        } catch (IOException e) {
            LOG.error("Scroll failed: " + e.getMessage());
        }

        if (searchResponse != null) {
            currentScrollWindowId = searchResponse.f0;
            currentScrollWindowHits = searchResponse.f1;
            nextRecordIndex = 0;
        }
    }

    private T parseSearchHit(String hit) {
        T row = null;
        try {
            row = deserializationSchema.deserialize(hit.getBytes());
        } catch (IOException e) {
            LOG.error("Deserialize search hit failed: " + e.getMessage());
        }

        return row;
    }
}
