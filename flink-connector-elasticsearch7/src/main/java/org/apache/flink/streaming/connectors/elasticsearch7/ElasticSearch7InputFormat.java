package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticSearchInputFormatBase;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@PublicEvolving
public class ElasticSearch7InputFormat<T>
        extends ElasticSearchInputFormatBase<T, RestHighLevelClient> {

    private static final long serialVersionUID = 1L;

    public ElasticSearch7InputFormat(
            Map<String, String> userConfig,
            List<HttpHost> httpHosts,
            RestClientFactory restClientFactory,
            DeserializationSchema<T> serializationSchema,
            String[] fieldNames,
            String index,
            long scrollTimeout,
            int scrollSize,
            QueryBuilder predicate,
            int limit) {

        super(
                new Elasticsearch7ApiCallBridge(httpHosts, restClientFactory),
                userConfig,
                serializationSchema,
                fieldNames,
                index,
                null,
                scrollTimeout,
                scrollSize,
                predicate,
                limit);
    }

    /**
     * A builder for creating an {@link ElasticSearch7InputFormat}.
     *
     * @param <T> Type of the elements.
     */
    @PublicEvolving
    public static class Builder<T> {
        private Map<String, String> userConfig = new HashMap<>();
        private List<HttpHost> httpHosts;
        private RestClientFactory restClientFactory = restClientBuilder -> {};
        private DeserializationSchema<T> deserializationSchema;
        private String index;

        private long scrollTimeout;
        private int scrollMaxSize;

        private String[] fieldNames;
        private QueryBuilder predicate;
        private int limit;

        public Builder() {}

        /**
         * Sets HttpHost which the RestHighLevelClient connects to.
         *
         * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient}
         *     connects to.
         */
        public Builder setHttpHosts(List<HttpHost> httpHosts) {
            this.httpHosts = httpHosts;
            return this;
        }

        /**
         * Sets a REST client factory for custom client configuration.
         *
         * @param restClientFactory the factory that configures the rest client.
         */
        public Builder setRestClientFactory(RestClientFactory restClientFactory) {
            this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
            return this;
        }

        public Builder setDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public Builder setIndex(String index) {
            this.index = index;
            return this;
        }

        /**
         * Sets the maxinum number of each Elasticsearch scroll request.
         *
         * @param scrollMaxSize the maxinum number of each Elasticsearch scroll request.
         */
        public Builder setScrollMaxSize(int scrollMaxSize) {
            Preconditions.checkArgument(
                    scrollMaxSize > 0,
                    "Maximum number each Elasticsearch scroll request must be larger than 0.");

            this.scrollMaxSize = scrollMaxSize;
            return this;
        }

        /**
         * Sets the search context alive for scroll requests, in milliseconds.
         *
         * @param scrollTimeout the search context alive for scroll requests, in milliseconds.
         */
        public Builder setScrollTimeout(long scrollTimeout) {
            Preconditions.checkArgument(
                    scrollTimeout >= 0,
                    "Yhe search context alive for scroll requests must be larger than or equal to 0.");

            this.scrollTimeout = scrollTimeout;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setPredicate(QueryBuilder predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder setLimit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Creates the ElasticSearch7RowDataInputFormat.
         *
         * @return the created ElasticSearch7RowDataInputFormat.
         */
        public ElasticSearch7InputFormat<T> build() {
            return new ElasticSearch7InputFormat<T>(
                    userConfig,
                    httpHosts,
                    restClientFactory,
                    deserializationSchema,
                    fieldNames,
                    index,
                    scrollTimeout,
                    scrollMaxSize,
                    predicate,
                    limit);
        }
    }
}
