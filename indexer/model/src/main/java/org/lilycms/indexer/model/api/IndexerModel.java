package org.lilycms.indexer.model.api;

import java.util.Collection;

public interface IndexerModel {    
    Collection<IndexDefinition> getIndexes();

    /**
     * Gets the list of indexes, and registers a listener for future changes to the indexes. It guarantees
     * that the listener will receive events for all updates that happened after the returned snapshot
     * of the indexes.
     *
     * <p>In case you are familiar with ZooKeeper, note that the listener does not work like the watcher
     * in ZooKeeper: listeners are not one-time only.
     */
    Collection<IndexDefinition> getIndexes(IndexerModelListener listener);

    IndexDefinition getIndex(String name) throws IndexNotFoundException;

    boolean hasIndex(String name);

    void registerListener(IndexerModelListener listener);

    void unregisterListener(IndexerModelListener listener);
}
