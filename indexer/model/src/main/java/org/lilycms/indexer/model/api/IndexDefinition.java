package org.lilycms.indexer.model.api;

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
