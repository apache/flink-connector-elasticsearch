package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.core.io.LocatableInputSplit;

public class ElasticsearchInputSplit extends LocatableInputSplit {

    private static final long seriaVersionUID = 1L;

    /** The name of the index to retrieve data from. */
    private final String index;

    /** It is null in flink elasticsearch connector 7+. */
    private final String type;

    /** Index will split diffirent shards when index created. */
    private final int shard;

    public ElasticsearchInputSplit(int splitNumber, String[] hostnames, String index, String type, int shard) {
        super(splitNumber, hostnames);
        this.index = index;
        this.type = type;
        this.shard = shard;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public int getShard() {
        return shard;
    }
}
