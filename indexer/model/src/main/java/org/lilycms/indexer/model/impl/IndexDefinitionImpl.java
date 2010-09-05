package org.lilycms.indexer.model.impl;

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

    public void makeImmutable() {
        this.immutable = true;
    }

    public void checkIfMutable() {
        if (immutable)
            throw new RuntimeException("This IndexDefinition is immutable");
    }
}
