package org.lilycms.indexer.model.api;

public class IndexerModelEvent {
    private IndexerModelEventType type;
    private String indexName;

    public IndexerModelEvent(IndexerModelEventType type, String indexName) {
        this.type = type;
        this.indexName = indexName;
    }

    public IndexerModelEventType getType() {
        return type;
    }

    public String getIndexName() {
        return indexName;
    }
}
