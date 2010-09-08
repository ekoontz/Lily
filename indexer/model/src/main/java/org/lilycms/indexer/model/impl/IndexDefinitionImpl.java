package org.lilycms.indexer.model.impl;

import org.lilycms.indexer.model.api.ActiveBuildJobInfo;
import org.lilycms.indexer.model.api.BuildJobInfo;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.IndexState;

import java.util.ArrayList;
import java.util.List;

public class IndexDefinitionImpl implements IndexDefinition {
    private String name;
    private IndexState state;
    private String messageConsumerId;
    private byte[] configuration;
    private List<String> solrShards;
    private int zkDataVersion = -1;
    private BuildJobInfo lastBuildJobInfo;
    private ActiveBuildJobInfo activeBuildJobInfo;
    private boolean immutable;

    public IndexDefinitionImpl(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public IndexState getState() {
        return state;
    }

    public void setState(IndexState state) {
        checkIfMutable();
        this.state = state;
    }

    public String getMessageConsumerId() {
        return messageConsumerId;
    }

    public void setMessageConsumerId(String messageConsumerId) {
        checkIfMutable();
        this.messageConsumerId = messageConsumerId;
    }

    public byte[] getConfiguration() {
        return configuration;
    }

    public void setConfiguration(byte[] configuration) {
        this.configuration = configuration;
    }

    public List<String> getSolrShards() {
        return new ArrayList<String>(solrShards);
    }

    public void setSolrShards(List<String> shards) {
        this.solrShards = new ArrayList<String>(shards);
    }

    public int getZkDataVersion() {
        return zkDataVersion;
    }

    public void setZkDataVersion(int zkDataVersion) {
        checkIfMutable();
        this.zkDataVersion = zkDataVersion;
    }

    public BuildJobInfo getLastBuildJobInfo() {
        return lastBuildJobInfo;
    }

    public void setLastBuildJobInfo(BuildJobInfo info) {
        checkIfMutable();
        this.lastBuildJobInfo = info;
    }

    public ActiveBuildJobInfo getActiveBuildJobInfo() {
        return activeBuildJobInfo;
    }

    public void setActiveBuildJobInfo(ActiveBuildJobInfo info) {
        checkIfMutable();
        this.activeBuildJobInfo = info;
    }

    public void makeImmutable() {
        this.immutable = true;
        if (lastBuildJobInfo != null)
            lastBuildJobInfo.makeImmutable();
        if (activeBuildJobInfo != null)
            activeBuildJobInfo.makeImmutable();
    }

    private void checkIfMutable() {
        if (immutable)
            throw new RuntimeException("This IndexDefinition is immutable");
    }
}
