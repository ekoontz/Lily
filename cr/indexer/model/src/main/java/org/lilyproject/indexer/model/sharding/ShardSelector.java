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

import org.lilyproject.repository.api.RecordId;

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
