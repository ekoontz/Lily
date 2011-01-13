/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.rowlog.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.rowlog.api.RowLogProcessor;
import org.lilyproject.util.zookeeper.LeaderElection;
import org.lilyproject.util.zookeeper.LeaderElectionCallback;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

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
        String electionPath = "/lily/rowlog/" + rowLogProcessor.getRowLog().getId() + "/masters";
        leaderElection = new LeaderElection(zk, "RowLog Processor " + rowLogProcessor.getRowLog().getId(),
                electionPath, new MyLeaderElectionCallback());
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
