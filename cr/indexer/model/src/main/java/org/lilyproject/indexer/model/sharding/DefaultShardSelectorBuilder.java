/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.model.sharding;

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
