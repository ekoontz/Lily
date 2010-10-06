package org.lilycms.util.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import static org.apache.zookeeper.ZooKeeper.States.*;

import javax.annotation.PreDestroy;

import static org.apache.zookeeper.Watcher.Event.KeeperState.*;

import java.io.IOException;

/**
 * This implementation of {@link ZooKeeperItf} is meant for use as a global ZooKeeper handle
 * within a ZK-dependent application.
 *
 * <p>It will:
 *
 * <ul>
 *   <li>on startup (= constructor) wait for the ZK connection to come up, if it does not
 *       come up within the session timeout an exception will be thrown. This avoids the
 *       remainder of the application starting up in the absence of a valid ZK connection.
 *   <li>when the session expires or the ZK connection is lost for longer than the session
 *       timeout, it will shut down the application.
 * </ul>
 *
 * <p>So this is a good solution for applications which can not function in absence of ZooKeeper.
 */
public class StateWatchingZooKeeper extends ZooKeeperImpl {
    private Log log = LogFactory.getLog(getClass());

    private int requestedSessionTimeout;

    private int sessionTimeout;

    /**
     * Ready becomes true once the ZooKeeper delegate has been set.
     */
    private volatile boolean ready;

    private volatile boolean stopping;

    private volatile boolean connected;

    private boolean firstConnect = true;

    private Thread stateWatcherThread;

    public StateWatchingZooKeeper(String connectString, int sessionTimeout) throws IOException {
        this.requestedSessionTimeout = sessionTimeout;
        this.sessionTimeout = sessionTimeout;

        ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, new MyWatcher());
        setDelegate(zk);
        ready = true;

        // Wait for connection to come up: if we fail to connect to ZK now, we do not want to continue
        // starting up the Lily node.
        long waitUntil = System.currentTimeMillis() + sessionTimeout;
        while (zk.getState() != CONNECTED && waitUntil > System.currentTimeMillis()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }

        if (zk.getState() != CONNECTED) {
            stopping = true;
            try {
                zk.close();
            } catch (Throwable t) {
                // ignore
            }
            throw new IOException("Failed to connect with Zookeeper within timeout " + sessionTimeout +
                    ", connection string: " + connectString);
        }

        log.info("ZooKeeper session ID is 0x" + Long.toHexString(zk.getSessionId()));
    }

    @PreDestroy
    public void stop() {
        stopping = true;
        if (stateWatcherThread != null) {
            stateWatcherThread.interrupt();
        }
        close();
    }

    private void endProcess(String message) {
        if (stopping)
            return;

        log.error(message);
        System.err.println(message);
        System.exit(1);
    }

    private class MyWatcher implements Watcher {

        public void process(WatchedEvent event) {
            if (stopping) {
                return;
            }

            try {
                if (event.getState() == Expired) {
                    endProcess("ZooKeeper session expired, shutting down.");
                } else if (event.getState() == Disconnected) {
                    log.warn("Disconnected from ZooKeeper");
                    connected = false;
                    waitForZk();
                    if (stateWatcherThread != null) {
                        stateWatcherThread.interrupt();
                    }
                    stateWatcherThread = new Thread(new StateWatcher(), "LilyZkStateWatcher");
                    stateWatcherThread.start();
                } else if (event.getState() == SyncConnected) {
                    if (firstConnect) {
                        firstConnect = false;
                        // For the initial connection, it is not interesting to log that we are connected.
                    } else {
                        log.warn("Connected to ZooKeeper");
                    }
                    connected = true;
                    waitForZk();
                    if (stateWatcherThread != null) {
                        stateWatcherThread.interrupt();
                        stateWatcherThread = null;
                    }
                    sessionTimeout = getSessionTimeout();
                    if (sessionTimeout != requestedSessionTimeout) {
                        log.info("The negotatiated ZooKeeper session timeout is different from the requested one." +
                                " Requested: " + requestedSessionTimeout + ", negotiated: " + sessionTimeout);
                    }
                }
            } catch (InterruptedException e) {
                // someone wants us to stop
                return;
            }
        }

        private void waitForZk() throws InterruptedException {
            while (!ready) {
                log.debug("Still waiting for reference to ZooKeeper.");
                Thread.sleep(5);
            }
        }
    }

    private class StateWatcher implements Runnable {

        private long startNotConnected;

        public void run() {
            startNotConnected = System.currentTimeMillis();

            while (true) {
                // We do not use ZooKeeper.getState() here, because I noticed that when we get a DisConnected
                // event in the watcher, the state still takes some time to move to CONNECTING.

                if (connected) {
                    // We are connected again, so we should not longer bother watching the state
                    return;
                }

                // Using a margin of twice the session timeout per
                // http://markmail.org/thread/uvefxjnuliuqwwph
                int margin = sessionTimeout * 2;
                if (startNotConnected + margin < System.currentTimeMillis()) {
                    endProcess("ZooKeeper connection lost for longer than " + margin +
                            " ms. Session will already be expired by server so shutting down.");
                    return;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // Someone requested us to stop
                    return;
                }
            }
        }
    }
}
