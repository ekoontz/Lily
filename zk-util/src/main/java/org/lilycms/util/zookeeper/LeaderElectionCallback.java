package org.lilycms.util.zookeeper;

/**
 * Used by {@link LeaderElection} to report leader election status.
 *
 * <p>The methods are called from within a ZooKeeper Watcher event callback, so be careful what
 * you do in the implementation (should be short-running + not wait for ZK events itself). 
 */
public interface LeaderElectionCallback {
    void elected();

    void noLongerElected();
}
