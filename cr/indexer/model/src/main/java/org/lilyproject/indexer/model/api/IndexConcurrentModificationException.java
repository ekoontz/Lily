package org.lilyproject.indexer.model.api;

public class IndexConcurrentModificationException extends Exception {
    public IndexConcurrentModificationException(String indexName) {
        super("The index is modified since it was read. Index: " + indexName);
    }
}
