package org.lilyproject.indexer.model.sharding;

public class ShardSelectorException extends Exception {
    public ShardSelectorException(String message) {
        super(message);
    }

    public ShardSelectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
