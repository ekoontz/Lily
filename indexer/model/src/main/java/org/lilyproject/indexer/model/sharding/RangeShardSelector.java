package org.lilyproject.indexer.model.sharding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RangeShardSelector extends BaseShardSelector {
    private List<ShardMappingEntry> mappings = new ArrayList<ShardMappingEntry>();

    public RangeShardSelector(ShardingKey shardingKey) {
        super(shardingKey);
    }

    protected void addMapping(String shardName, Comparable maxValue) {
        mappings.add(new ShardMappingEntry(shardName, maxValue));
    }

    public String getShard(Comparable key) throws ShardSelectorException {
        for (ShardMappingEntry mapping : mappings) {
            if (mapping.maxValue == null) {
                return mapping.shardName;
            } else if (key.compareTo(mapping.maxValue) < 0) {
                return mapping.shardName;
            }
        }

        throw new ShardSelectorException("Shard key does not map onto a shard: " + key);
    }

    private static class ShardMappingEntry {
        Comparable maxValue;
        String shardName;

        public ShardMappingEntry(String shardName, Comparable maxValue) {
            this.shardName = shardName;
            this.maxValue = maxValue;
        }
    }

    public Set<String> getShards() {
        Set<String> shards = new HashSet<String>();
        for (ShardMappingEntry entry : mappings) {
            shards.add(entry.shardName);
        }
        return shards;
    }
}
