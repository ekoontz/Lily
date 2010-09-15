package org.lilycms.indexer.model.sharding;

import org.lilycms.repository.api.RecordId;

public abstract class BaseShardSelector implements ShardSelector {
    private ShardingKey shardingKey;

    public BaseShardSelector(ShardingKey shardingKey) {
        this.shardingKey = shardingKey;
    }
    
    public String getShard(RecordId recordId) throws ShardSelectorException {
        Comparable key = shardingKey.getShardingKey(recordId);
        return getShard(key);
    }

    abstract String getShard(Comparable key) throws ShardSelectorException;
}
