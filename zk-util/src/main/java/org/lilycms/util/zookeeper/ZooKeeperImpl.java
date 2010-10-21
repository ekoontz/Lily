package org.lilycms.util.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;

/**
 * Default implementation of {@link ZooKeeperItf}.
 *
 * <p>To wait until the ZK connection is established, use {@link ZkUtil#connect(String, int)}.
 *
 * <p>For a global ZK handle to be used by a ZK-dependent application, see rather
 * {@link org.lilycms.util.zookeeper.StateWatchingZooKeeper}.
 */
public class ZooKeeperImpl implements ZooKeeperItf {
    protected ZooKeeper delegate;

    protected Set<Watcher> additionalDefaultWatchers = Collections.newSetFromMap(new IdentityHashMap<Watcher, Boolean>());

    protected boolean connected = false;

    protected volatile boolean stop = false;

    protected final Object connectedMonitor = new Object();

    protected Thread zkEventThread;

    private Log log = LogFactory.getLog(getClass());

    protected ZooKeeperImpl() {

    }

    protected void setDelegate(ZooKeeper delegate) {
        this.delegate = delegate;
    }

    public ZooKeeperImpl(String connectString, int sessionTimeout) throws IOException {
        this.delegate = new ZooKeeper(connectString, sessionTimeout, new MyWatcher());
    }

    public void addDefaultWatcher(Watcher watcher) {
        additionalDefaultWatchers.add(watcher);
    }

    public void removeDefaultWatcher(Watcher watcher) {
        additionalDefaultWatchers.remove(watcher);
    }

    public void shutdown() {
        this.stop = true;
        synchronized (connectedMonitor) {
            connectedMonitor.notifyAll();
        }
    }

    public void waitForConnection() throws InterruptedException {
        if (isCurrentThreadEventThread()) {
            throw new RuntimeException("waitForConnection should not be called from within the ZooKeeper event thread.");
        }

        synchronized (connectedMonitor) {
            while (!connected && !stop) {
                connectedMonitor.wait();
            }
        }

        if (stop) {
            throw new InterruptedException("This ZooKeeper handle is shutting down.");
        }
    }

    public boolean isCurrentThreadEventThread() {
        // Disclaimer: this way of detected wrong use of the event thread was inspired by the ZKClient library.
        return zkEventThread != null && zkEventThread == Thread.currentThread();
    }

    protected void setConnectedState(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            synchronized (connectedMonitor) {
                if (!connected) {
                    connected = true;
                    connectedMonitor.notifyAll();
                }
            }
        } else if (event.getState() == Watcher.Event.KeeperState.Disconnected ||
                event.getState() == Watcher.Event.KeeperState.Expired) {
            synchronized (connectedMonitor) {
                if (connected) {
                    connected = false;
                    connectedMonitor.notifyAll();
                }
            }
        }
    }

    public <T> T retryOperation(ZooKeeperOperation<T> operation) throws InterruptedException, KeeperException {
        if (isCurrentThreadEventThread()) {
            throw new RuntimeException("retryOperation should not be called from within the ZooKeeper event thread.");
        }

        int tryCount = 0;

        while (true) {
            tryCount++;

            try {
                return operation.execute();
            } catch (KeeperException.ConnectionLossException e) {
                // ok
            }

            if (tryCount > 3) {
                log.warn("ZooKeeper operation attempt " + tryCount + " failed due to connection loss.");
            }

            waitForConnection();
        }
    }

    public long getSessionId() {
        return delegate.getSessionId();
    }

    public byte[] getSessionPasswd() {
        return delegate.getSessionPasswd();
    }

    public int getSessionTimeout() {
        return delegate.getSessionTimeout();
    }

    public void addAuthInfo(String scheme, byte[] auth) {
        delegate.addAuthInfo(scheme, auth);
    }

    public void register(Watcher watcher) {
        delegate.register(watcher);
    }

    public void close() {
        try {
            delegate.close();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException(e);
        }
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        return delegate.create(path, data, acl, createMode);
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        delegate.create(path, data, acl, createMode, cb, ctx);
    }

    public void delete(String path, int version) throws InterruptedException, KeeperException {
        delegate.delete(path, version);
    }

    public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
        delegate.delete(path, version, cb, ctx);
    }

    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return delegate.exists(path, watcher);
    }

    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return delegate.exists(path, watch);
    }

    public void exists(String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.exists(path, watcher, cb, ctx);
    }

    public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.exists(path, watch, cb, ctx);
    }

    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getData(path, watcher, stat);
    }

    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getData(path, watch, stat);
    }

    public void getData(String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
        delegate.getData(path, watcher, cb, ctx);
    }

    public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
        delegate.getData(path, watch, cb, ctx);
    }

    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return delegate.setData(path, data, version);
    }

    public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.setData(path, data, version, cb, ctx);
    }

    public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getACL(path, stat);
    }

    public void getACL(String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx) {
        delegate.getACL(path, stat, cb, ctx);
    }

    public Stat setACL(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        return delegate.setACL(path, acl, version);
    }

    public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.setACL(path, acl, version, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watcher);
    }

    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watch);
    }

    public void getChildren(String path, Watcher watcher, AsyncCallback.ChildrenCallback cb, Object ctx) {
        delegate.getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
        delegate.getChildren(path, watch, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watcher, stat);
    }

    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watch, stat);
    }

    public void getChildren(String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx) {
        delegate.getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx) {
        delegate.getChildren(path, watch, cb, ctx);
    }

    public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
        delegate.sync(path, cb, ctx);
    }

    public ZooKeeper.States getState() {
        return delegate.getState();
    }

    public class MyWatcher implements Watcher {
        private boolean firstConnect = true;

        public void process(WatchedEvent event) {
            zkEventThread = Thread.currentThread();

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

            setConnectedState(event);

            for (Watcher watcher : additionalDefaultWatchers) {
                watcher.process(event);
            }
        }
    }

}
