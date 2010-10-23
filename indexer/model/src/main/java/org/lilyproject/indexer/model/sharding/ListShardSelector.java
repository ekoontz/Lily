package org.lilyproject.indexer.model.sharding;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ListShardSelector extends BaseShardSelector {
    private Map<Object, String> valueToShard = new HashMap<Object, String>();

    public ListShardSelector(ShardingKey shardingKey) {
        super(shardingKey);
    }

    protected void addMapping(Comparable value, String shardName) throws ShardingConfigException {
        String existingShard = valueToShard.get(value);
        if (existingShard != null) {
            throw new ShardingConfigException("Same value maps to multiple shards. Value: " + value + ", shards: " +
                    existingShard + " and " + shardName);
        }
        valueToShard.put(value, shardName);
    }

    public String getShard(Comparable key) throws ShardSelectorException {
        String shardName = valueToShard.get(key);
        if (shardName == null) {
            throw new ShardSelectorException("Shard value does not map to a shard: " + key);
        }
        return shardName;
    }

    public Set<String> getShards() {
        return new HashSet<String>(valueToShard.values());
    }
}
