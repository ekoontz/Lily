package org.lilycms.indexer.model.api;

import java.util.List;

public interface IndexDefinition {
    String getName();

    IndexState getState();

    void setState(IndexState state);

    String getMessageConsumerId();

    void setMessageConsumerId(String messageConsumerId);

    byte[] getConfiguration();

    void setConfiguration(byte[] configuration);

    List<String> getSolrShards();

    void setSolrShards(List<String> shards);

    int getZkDataVersion();

}
