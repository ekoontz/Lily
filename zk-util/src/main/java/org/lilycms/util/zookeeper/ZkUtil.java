package org.lilycms.util.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Various ZooKeeper utility methods.
 */
public class ZkUtil {
    
    public static ZooKeeperItf connect(String connectString, int sessionTimeout) throws ZkConnectException {
        try {
            ZooKeeperImpl zooKeeper = new ZooKeeperImpl(new ZooKeeper(connectString, sessionTimeout, new Watcher(){
                public void process(WatchedEvent event) {
                }}));
            long waitUntil = System.currentTimeMillis() + sessionTimeout;
            boolean connected = (States.CONNECTED).equals(zooKeeper.getState());
            while (!connected && waitUntil > System.currentTimeMillis()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    connected = (States.CONNECTED).equals(zooKeeper.getState());
                    break;
                }
                connected = (States.CONNECTED).equals(zooKeeper.getState());
            }
            if (!connected)
                throw new ZkConnectException("Failed to connect with Zookeeper @ <"+connectString+"> within timeout <"+sessionTimeout+">", null);
            return zooKeeper;
        } catch (IOException e) {
            throw new ZkConnectException("Failed to connect with Zookeeper @ <"+connectString+">", e);
        }
    }
    
    public static void createPath(ZooKeeper zk, String path) throws ZkPathCreationException {
        createPath(new ZooKeeperImpl(zk), path, null, CreateMode.PERSISTENT);
    }

    public static void createPath(ZooKeeper zk, String path, byte[] data, CreateMode createMode) throws ZkPathCreationException {
        createPath(new ZooKeeperImpl(zk), path, data, createMode);
    }

    public static void createPath(final ZooKeeperItf zk, final String path) throws ZkPathCreationException {
        createPath(zk, path, null, CreateMode.PERSISTENT);
    }

    /**
     * Creates a persistent path on zookeeper if it does not exist yet, including any parents.
     * Keeps retrying in case of connection loss.
     *
     */
    public static void createPath(final ZooKeeperItf zk, final String path, final byte[] data, final CreateMode createMode) throws ZkPathCreationException {
        try {
            Stat stat = retryOperationForever(new ZooKeeperOperation<Stat>() {
                public Stat execute() throws KeeperException, InterruptedException {
                    return zk.exists(path, null);
                }
            });

            if (stat != null)
                return;
        } catch (KeeperException e) {
            throw new ZkPathCreationException("Error testing path for existence: " + path, e);
        } catch (InterruptedException e) {
            throw new ZkPathCreationException("Error testing path for existence: " + path, e);
        }

        if (!path.startsWith("/"))
            throw new IllegalArgumentException("Path should start with a slash.");

        String[] parts = path.substring(1).split("/");

        final StringBuilder subPath = new StringBuilder();
        for (String part : parts) {
            subPath.append("/").append(part);
            try {
                retryOperationForever(new ZooKeeperOperation<String>() {
                    public String execute() throws KeeperException, InterruptedException {
                        return zk.create(subPath.toString(), data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
                    }
                });
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            } catch (InterruptedException e) {
                throw new ZkPathCreationException(getPathCreateFailureMessage(subPath.toString(), path), e);
            } catch (KeeperException e) {
                throw new ZkPathCreationException(getPathCreateFailureMessage(subPath.toString(), path), e);
            }
        }
    }

    private static String getPathCreateFailureMessage(String subPath, String path) {
        if (subPath.equals(path)) {
            return "Failed to create ZooKeeper path " + path;
        } else {
            return "Failed to create ZooKeeper path " + subPath + " while creating path " + path;
        }
    }

    /**
     * Perform the given operation, retrying in case of connection loss.
     *
     * @retryCount if -1, retries forever
     */
    public static <T> T retryOperation(ZooKeeperOperation<T> operation, int retryCount)
        throws KeeperException, InterruptedException {
        // Disclaimer: this method was copied from ZooKeeper's lock recipe (class ProtocolSupport) and slightly altered
        KeeperException exception = null;
        for (int i = 0; retryCount == -1 || i < retryCount; i++) {
            try {
                return operation.execute();
            } catch (KeeperException.ConnectionLossException e) {
                if (exception == null) {
                    exception = e;
                }
                Log log = LogFactory.getLog(ZkUtil.class);
                log.warn("ZooKeeper operation attempt " + i + " failed due to connection loss.", e);
                retryDelay(i);
            }
        }
        throw exception;
    }

    public static <T> T retryOperationForever(ZooKeeperOperation<T> operation)
            throws InterruptedException, KeeperException {
        return retryOperation(operation, -1);
    }

    /**
     * Performs a retry delay if this is not the first attempt
     * @param attemptCount the number of the attempts performed so far
     */
    private static void retryDelay(int attemptCount) {
        // Disclaimer: this method was copied from ZooKeeper's lock recipe (class ProtocolSupport) and slightly altered
        if (attemptCount > 0) {
            try {
                long delay = Math.min(attemptCount * 500L, 10000L);
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Log log = LogFactory.getLog(ZkUtil.class);
                log.debug("Failed to sleep: " + e, e);
            }
        }
    }
}
