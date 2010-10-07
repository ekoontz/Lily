package org.lilycms.indexer.model.impl;

import org.lilycms.indexer.model.api.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IndexDefinitionImpl implements IndexDefinition {
    private String name;
    private IndexGeneralState generalState = IndexGeneralState.ACTIVE;
    private IndexBatchBuildState buildState = IndexBatchBuildState.INACTIVE;
    private IndexUpdateState updateState = IndexUpdateState.SUBSCRIBE_AND_LISTEN;
    private String queueSubscriptionId;
    private byte[] configuration;
    private byte[] shardingConfiguration;
    private Map<String, String> solrShards = Collections.emptyMap();
    private int zkDataVersion = -1;
    private BatchBuildInfo lastBatchBuildInfo;
    private ActiveBatchBuildInfo activeBatchBuildInfo;
    private boolean immutable;

    public IndexDefinitionImpl(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public IndexGeneralState getGeneralState() {
        return generalState;
    }

    public void setGeneralState(IndexGeneralState state) {
        checkIfMutable();
        this.generalState = state;
    }

    public IndexBatchBuildState getBatchBuildState() {
        return buildState;
    }

    public void setBatchBuildState(IndexBatchBuildState state) {
        checkIfMutable();
        this.buildState = state;
    }

    public IndexUpdateState getUpdateState() {
        return updateState;
    }

    public void setUpdateState(IndexUpdateState state) {
        checkIfMutable();
        this.updateState = state;
    }

    public String getQueueSubscriptionId() {
        return queueSubscriptionId;
    }

    public void setQueueSubscriptionId(String queueSubscriptionId) {
        checkIfMutable();
        this.queueSubscriptionId = queueSubscriptionId;
    }

    public byte[] getConfiguration() {
        // Note that while one could modify the returned byte array, it is very unlikely to do this
        // by accident, and we assume cooperating users.
        return configuration;
    }

    public void setConfiguration(byte[] configuration) {
        this.configuration = configuration;
    }

    public byte[] getShardingConfiguration() {
        return shardingConfiguration;
    }

    public void setShardingConfiguration(byte[] shardingConfiguration) {
        this.shardingConfiguration = shardingConfiguration;
    }

    public Map<String, String> getSolrShards() {
        return new HashMap<String, String>(solrShards);
    }

    public void setSolrShards(Map<String, String> shards) {
        this.solrShards = new HashMap<String, String>(shards);
    }

    public int getZkDataVersion() {
        return zkDataVersion;
    }

    public void setZkDataVersion(int zkDataVersion) {
        checkIfMutable();
        this.zkDataVersion = zkDataVersion;
    }

    public BatchBuildInfo getLastBatchBuildInfo() {
        return lastBatchBuildInfo;
    }

    public void setLastBatchBuildInfo(BatchBuildInfo info) {
        checkIfMutable();
        this.lastBatchBuildInfo = info;
    }

    public ActiveBatchBuildInfo getActiveBatchBuildInfo() {
        return activeBatchBuildInfo;
    }

    public void setActiveBatchBuildInfo(ActiveBatchBuildInfo info) {
        checkIfMutable();
        this.activeBatchBuildInfo = info;
    }

    public void makeImmutable() {
        this.immutable = true;
        if (lastBatchBuildInfo != null)
            lastBatchBuildInfo.makeImmutable();
        if (activeBatchBuildInfo != null)
            activeBatchBuildInfo.makeImmutable();
    }

    private void checkIfMutable() {
        if (immutable)
            throw new RuntimeException("This IndexDefinition is immutable");
    }
}
