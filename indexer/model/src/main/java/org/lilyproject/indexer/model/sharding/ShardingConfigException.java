package org.lilyproject.indexer.model.sharding;

public class ShardingConfigException extends Exception {
    public ShardingConfigException(String message) {
        super(message);
    }

    public ShardingConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
