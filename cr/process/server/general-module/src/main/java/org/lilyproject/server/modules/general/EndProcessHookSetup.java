package org.lilyproject.server.modules.general;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;

import javax.annotation.PostConstruct;

public class EndProcessHookSetup {
    private StateWatchingZooKeeper zk;

    public EndProcessHookSetup(StateWatchingZooKeeper zk) {
        this.zk = zk;
    }

    @PostConstruct
    public void start() {
        zk.setEndProcessHook(new Runnable() {
            public void run() {
                HConnectionManager.hardStopRequested = true;
            }
        });
    }
}
