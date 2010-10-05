package org.lilycms.rowlog.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.util.zookeeper.LeaderElection;
import org.lilycms.util.zookeeper.LeaderElectionCallback;
import org.lilycms.util.zookeeper.LeaderElectionSetupException;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * Starts and stops a RowLogProcessor based on ZooKeeper-based leader election.
 */
public class RowLogProcessorElection {
    private LeaderElection leaderElection;

    private ZooKeeperItf zk;

    private RowLogProcessor rowLogProcessor;

    private final Log log = LogFactory.getLog(getClass());

    public RowLogProcessorElection(ZooKeeperItf zk, RowLogProcessor rowLogProcessor) {
        this.zk = zk;
        this.rowLogProcessor = rowLogProcessor;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        leaderElection = new LeaderElection(zk, "RowLog Processor " + rowLogProcessor.getRowLog().getId(),
                "/lily/indexer/masters", new MyLeaderElectionCallback());
    }

    @PreDestroy
    public void stop() {
        if (leaderElection != null) {
            try {
                leaderElection.stop();
                leaderElection = null;
            } catch (InterruptedException e) {
                log.info("Interrupted while shutting down leader election.");
            }
        }
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        public void activateAsLeader() throws Exception {
            log.info("Starting row log processor for " + rowLogProcessor.getRowLog().getId());
            rowLogProcessor.start();
            log.info("Startup of row log processor successful for " + rowLogProcessor.getRowLog().getId());
        }

        public void deactivateAsLeader() throws Exception {
            log.info("Shutting down row log processor for " + rowLogProcessor.getRowLog().getId());
            rowLogProcessor.stop();
            log.info("Shutdown of row log processor sucessful for " + rowLogProcessor.getRowLog().getId());
        }
    }
}
