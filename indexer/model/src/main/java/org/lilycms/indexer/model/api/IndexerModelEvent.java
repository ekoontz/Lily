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

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IndexerModelEvent other = (IndexerModelEvent) obj;
        return other.type == type && other.indexName.equals(indexName);
    }
}
