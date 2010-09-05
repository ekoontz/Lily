package org.lilycms.server.modules.general;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DummyWatcher implements Watcher {
    public void process(WatchedEvent event) {
    }
}
