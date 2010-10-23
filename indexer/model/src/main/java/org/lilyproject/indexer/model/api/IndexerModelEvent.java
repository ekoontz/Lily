package org.lilyproject.indexer.model.api;

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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + type.hashCode();
        result = prime * result + indexName.hashCode();
        return result;
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

    @Override
    public String toString() {
        return "Indexer model event [type = " + type + ", index = " + indexName + "]";
    }
}
