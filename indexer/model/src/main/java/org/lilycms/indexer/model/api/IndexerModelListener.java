package org.lilycms.indexer.model.api;

public interface IndexerModelListener {
    void process(IndexerModelEvent event);
}
