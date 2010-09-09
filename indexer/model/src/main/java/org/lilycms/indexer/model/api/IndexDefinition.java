package org.lilycms.indexer.model.api;

import java.util.List;

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

    byte[] getConfiguration();

    void setConfiguration(byte[] configuration);

    List<String> getSolrShards();

    void setSolrShards(List<String> shards);

    int getZkDataVersion();

    BatchBuildInfo getLastBatchBuildInfo();

    void setLastBatchBuildInfo(BatchBuildInfo info);

    ActiveBatchBuildInfo getActiveBatchBuildInfo();

    void setActiveBatchBuildInfo(ActiveBatchBuildInfo info);
}
