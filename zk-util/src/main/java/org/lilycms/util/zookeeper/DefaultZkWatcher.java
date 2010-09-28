package org.lilycms.util.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * A default ZK watcher to use as root watcher.
 */
public class DefaultZkWatcher implements Watcher {
    private boolean firstConnect = true;

    public void process(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.Disconnected) {
            System.err.println("ZooKeeper Disconnected.");
        } else if (event.getState() == Event.KeeperState.Expired) {
            System.err.println("ZooKeeper session expired.");
        } else if (event.getState() == Event.KeeperState.SyncConnected) {
            if (firstConnect) {
                // don't log the first time we connect.
                firstConnect = false;
            } else {
                System.out.println("ZooKeeper connected.");
            }
        }
    }
}
