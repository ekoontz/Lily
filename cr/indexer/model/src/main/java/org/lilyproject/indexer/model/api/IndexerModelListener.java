package org.lilyproject.indexer.model.api;

/**
 * Reports changes to the {@link IndexerModel}.
 *
 * <p>The methods are called from within a ZooKeeper Watcher event callback, so be careful what
 * you do in the implementation (should be short-running + not wait for ZK events itself). 
 */
public interface IndexerModelListener {
    void process(IndexerModelEvent event);
}
