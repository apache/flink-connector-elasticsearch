package org.apache.flink.streaming.tests;

import org.apache.flink.util.DockerImageVersions;

public class Elasticsearch7LookupE2ECase extends ElasticsearchLookupE2ECase {
    @Override
    String getElasticsearchContainerName() {
        return DockerImageVersions.ELASTICSEARCH_7;
    }

    @Override
    String getEsOptions() {
        return " 'connector' = 'elasticsearch-7',"
                + " 'hosts' = '"
                + "http://"
                + elasticsearchContainer.getHttpHostAddress()
                + "',"
                + "'index' = '"
                + es_index
                + "',"
                + "'lookup.cache' = 'partial',"
                + "'lookup.partial-cache.max-rows' = '100'";
    }
}
