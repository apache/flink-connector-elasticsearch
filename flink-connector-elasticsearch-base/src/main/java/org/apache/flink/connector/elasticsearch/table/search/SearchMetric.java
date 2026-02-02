package org.apache.flink.connector.elasticsearch.table.search;

/** Metric for vector search. */
public enum SearchMetric {
    COSINE_SIMILARITY("cosineSimilarity"),
    L1NORM("l1norm"),
    L2NORM("l2norm"),
    HAMMING("hamming"),
    DOT_PRODUCT("dotProduct");

    private final String name;

    SearchMetric(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
