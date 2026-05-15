package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.elasticsearch.table.search.SearchMetric;

/**
 * Options specific for the Elasticsearch 7 connector. Public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class Elasticsearch7ConnectorOptions extends ElasticsearchConnectorOptions {
    private Elasticsearch7ConnectorOptions() {}

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys("lookup.max-retries")
                    .withDescription(
                            "The maximum allowed retries if a lookup/search operation fails.");

    public static final ConfigOption<SearchMetric> VECTOR_SEARCH_METRIC =
            ConfigOptions.key("vector-search.metric")
                    .enumType(SearchMetric.class)
                    .defaultValue(SearchMetric.COSINE_SIMILARITY)
                    .withDescription(
                            "The metric of vector search, by default is cosineSimilarity.");
}
