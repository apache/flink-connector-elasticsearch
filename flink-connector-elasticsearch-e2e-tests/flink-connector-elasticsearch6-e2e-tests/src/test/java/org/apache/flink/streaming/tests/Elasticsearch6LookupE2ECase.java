package org.apache.flink.streaming.tests;

import org.apache.flink.util.DockerImageVersions;

public class Elasticsearch6LookupE2ECase extends ElasticsearchLookupE2ECase {
    @Override
    String getElasticsearchContainerName() {
        return DockerImageVersions.ELASTICSEARCH_6;
    }

    @Override
    String getEsOptions() {
        return " 'connector' = 'elasticsearch-6',"
                + " 'hosts' = '"
                + "http://"
                + elasticsearchContainer.getHttpHostAddress()
                + "',"
                + "'document-type' = '_doc',"
                + "'index' = '"
                + es_index
                + "',"
                + "'lookup.cache' = 'partial',"
                + "'lookup.partial-cache.max-rows' = '100'";
    }
}
