package org.lilycms.indexer.model.sharding;

import java.util.*;

public class DefaultShardSelectorBuilder {
    public static ShardSelector createDefaultSelector(Map<String, String> solrShards) throws ShardingConfigException {
        // Default config: shard on the hash of the master record id modulus number of shards

        ShardingKey key = ShardingKey.masterRecordIdShardingKey(true, solrShards.size(), ShardingKey.KeyType.LONG);

        ListShardSelector selector = new ListShardSelector(key);

        int i = 0;
        for (String shard : new TreeSet<String>(solrShards.keySet())) {
            selector.addMapping(new Long(i), shard);
            i++;
        }

        return selector;
    }
}
