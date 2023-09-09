package org.apache.flink.connector.elasticsearch.bulk;

import org.apache.flink.annotation.PublicEvolving;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

/**
 * Users add multiple delete, index or update requests to a {@link
 * org.apache.flink.connector.elasticsearch.bulk.RequestIndexer} to prepare them for sending to an
 * Elasticsearch cluster.
 */
@PublicEvolving
public interface RequestIndexer {
    /**
     * Add multiple {@link BulkOperation} to the indexer to prepare for sending requests to
     * Elasticsearch.
     *
     * @param operations The multiple {@link BulkOperation} to add.
     */
    void add(BulkOperation... operations);
}
