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
package org.lilyproject.indexer.model.api;

import java.util.Map;

public interface IndexDefinition {
    String getName();

    IndexGeneralState getGeneralState();

    void setGeneralState(IndexGeneralState state);
    
    IndexBatchBuildState getBatchBuildState();

    void setBatchBuildState(IndexBatchBuildState state);

    /**
     * If the state implies that there should be a message queue subscription, check
     * {@link #getQueueSubscriptionId()} to see if the subscription is already assigned,
     * same for unsubscribe.
     */
    IndexUpdateState getUpdateState();

    void setUpdateState(IndexUpdateState state);

    String getQueueSubscriptionId();

    void setQueueSubscriptionId(String queueSubscriptionId);

    /**
     * The XML configuration for the Indexer.
     */
    byte[] getConfiguration();

    void setConfiguration(byte[] configuration);

    /**
     * The JSON configuration for the shard selector.
     */
    byte[] getShardingConfiguration();

    void setShardingConfiguration(byte[] configuration);

    /**
     * Map containing the SOLR shards: the key is a logical name for the shard, the value is the
     * address (URL) of the shard.
     */
    Map<String, String> getSolrShards();

    void setSolrShards(Map<String, String> shards);

    int getZkDataVersion();

    BatchBuildInfo getLastBatchBuildInfo();

    void setLastBatchBuildInfo(BatchBuildInfo info);

    ActiveBatchBuildInfo getActiveBatchBuildInfo();

    void setActiveBatchBuildInfo(ActiveBatchBuildInfo info);
}
