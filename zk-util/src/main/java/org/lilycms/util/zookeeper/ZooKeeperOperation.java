package org.lilycms.util.zookeeper;

import org.apache.zookeeper.KeeperException;


// Disclaimer: this interface was copied from ZooKeeper's lock recipe and slightly altered.

/**
 * A callback object which can be used for implementing retry-able operations, used by
 * {@link ZooKeeperItf#retryOperation}.
 *
 */
public interface ZooKeeperOperation<T> {
    /**
     * Performs the operation - which may be involved multiple times if the connection
     * to ZooKeeper closes during this operation
     *
     */
    public T execute() throws KeeperException, InterruptedException;
}
