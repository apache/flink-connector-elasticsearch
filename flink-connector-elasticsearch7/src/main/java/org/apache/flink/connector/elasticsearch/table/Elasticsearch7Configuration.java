package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.elasticsearch.table.search.SearchMetric;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7ConnectorOptions.MAX_RETRIES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7ConnectorOptions.VECTOR_SEARCH_METRIC;

/** Elasticsearch 7 specific configuration. */
public class Elasticsearch7Configuration extends ElasticsearchConfiguration {
    Elasticsearch7Configuration(ReadableConfig config) {
        super(config);
    }

    public int getMaxRetries() {
        return config.get(MAX_RETRIES);
    }

    public SearchMetric getVectorSearchMetric() {
        return config.get(VECTOR_SEARCH_METRIC);
    }
}
