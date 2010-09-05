package org.lilycms.indexer.model.api;

import org.apache.zookeeper.KeeperException;
import org.lilycms.util.zookeeper.ZkLockException;

public interface WriteableIndexerModel extends IndexerModel {

    /**
     * Instantiates an IndexDefinition object, but does not register it yet, you should
     * do this using {@link #addIndex}.
     */
    IndexDefinition newIndex(String name);

    void addIndex(IndexDefinition index) throws IndexExistsException, IndexModelException;

    /**
     * Loads an index definition and returns it in a mutable way.
     *
     * <p>This differs from {@link #getIndex(String)} in that the returned index definition
     * is mutable (updateable) and it is also freshly loaded from storage.
     */
    IndexDefinition getMutableIndex(String name) throws InterruptedException, KeeperException, IndexNotFoundException;

    /**
     * Updates an index.
     *
     * <p>The update will only succeed if it was not modified since it was read. This situation can be avoided
     * by taking a lock on the index.
     */
    void updateIndex(IndexDefinition index) throws InterruptedException, KeeperException, IndexNotFoundException,
            IndexConcurrentModificationException;

    /**
     * Takes a lock on this index.
     *
     * <p>Taking a lock can avoid concurrent modification exceptions when updating the index.
     *
     * <p>TODO: can/should clients use this lock for their own purposes?
     */
    String lockIndex(String indexName) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException;

    void unlockIndex(String lock) throws ZkLockException;
}
