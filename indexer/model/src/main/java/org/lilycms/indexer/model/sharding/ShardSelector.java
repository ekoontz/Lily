package org.lilycms.indexer.model.sharding;

import org.lilycms.repository.api.RecordId;

import java.util.Set;

public interface ShardSelector {
    /**
     * Returns the name of the shard to use for the given record ID.
     */
    String getShard(RecordId recordId) throws ShardSelectorException;

    /**
     * Returns the set of shard names used by this selector. Can be used for validation
     * purposes, such as verifying whether all referred shards exist or whether all existing shards
     * are used by the selector.
     */
    Set<String> getShards();
}
