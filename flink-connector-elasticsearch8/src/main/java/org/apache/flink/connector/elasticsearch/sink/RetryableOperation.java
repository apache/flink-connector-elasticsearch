package org.apache.flink.connector.elasticsearch.sink;

import java.io.Serializable;

/**
 * A wrapper around Operation that tracks how many times it has been attempted.
 */
public class RetryableOperation implements Serializable {
    private final Operation operation;
    private int attemptCount;

    public RetryableOperation(Operation operation) {
        this.operation = operation;
        this.attemptCount = 0;
    }

    public Operation getOperation() {
        return operation;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void incrementAttempt() {
        this.attemptCount++;
    }

    @Override
    public String toString() {
        return "RetryableOperation{attempts=" + attemptCount + ", op=" + operation + "}";
    }
}
