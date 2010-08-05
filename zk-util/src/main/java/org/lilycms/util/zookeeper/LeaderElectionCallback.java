package org.lilycms.util.zookeeper;

/**
 * Used by {@link LeaderElection} to report leader election status.
 */
public interface LeaderElectionCallback {
    void elected();

    void noLongerElected();
}
