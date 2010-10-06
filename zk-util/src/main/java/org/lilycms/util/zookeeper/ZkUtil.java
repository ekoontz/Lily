package org.lilycms.util.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Various ZooKeeper utility methods.
 */
public class ZkUtil {
    
    public static ZooKeeperItf connect(String connectString, int sessionTimeout) throws ZkConnectException {
        ZooKeeperImpl zooKeeper;
        try {
            zooKeeper = new ZooKeeperImpl(new ZooKeeper(connectString, sessionTimeout, new DefaultZkWatcher()));
        } catch (IOException e) {
            throw new ZkConnectException("Failed to connect with Zookeeper @ <" + connectString + ">", e);
        }
        long waitUntil = System.currentTimeMillis() + 10000;
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
        if (!connected) {
            System.out.println("Failed to connect to Zookeeper within timeout: Dumping stack: ");
            Thread.dumpStack();
            zooKeeper.close();
            throw new ZkConnectException("Failed to connect with Zookeeper @ <" + connectString +
                    "> within timeout <" + sessionTimeout + ">");
        }
        return zooKeeper;
    }

    public static void createPath(final ZooKeeperItf zk, final String path)
            throws InterruptedException, KeeperException {
        createPath(zk, path, null, CreateMode.PERSISTENT);
    }

    /**
     * Creates a persistent path on zookeeper if it does not exist yet, including any parents.
     * Keeps retrying in case of connection loss.
     *
     */
    public static void createPath(final ZooKeeperItf zk, final String path, final byte[] data,
            final CreateMode createMode) throws InterruptedException, KeeperException {

        Stat stat = retryOperation(new ZooKeeperOperation<Stat>() {
            public Stat execute() throws KeeperException, InterruptedException {
                return zk.exists(path, null);
            }
        });

        if (stat != null)
            return;

        if (!path.startsWith("/"))
            throw new IllegalArgumentException("Path should start with a slash.");

        String[] parts = path.substring(1).split("/");

        final StringBuilder subPath = new StringBuilder();
        for (String part : parts) {
            subPath.append("/").append(part);
            try {
                retryOperation(new ZooKeeperOperation<String>() {
                    public String execute() throws KeeperException, InterruptedException {
                        return zk.create(subPath.toString(), data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
                    }
                });
            } catch (KeeperException.NodeExistsException e) {
                // ignore
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
     * <p>Note that in case of connection loss, you are never sure if the operation succeeded or not,
     * so it might be executed twice. Therefore:
     *
     * <ul>
     *   <li>in case of a delete operation, be prepared to deal with a NoNode exception
     *   <li>in case of a create operation, be prepared to deal with a NodeExists exception
     *   <li>in case of creation of a sequential node, two nodes might have been created. If they are ephemeral,
     *       you can use Stat.ephemeralOwner to find out the ones that belong to the current session. Otherwise,
     *       embed the necessary identification into the name or data.
     * </ul>
     *
     * <p>Do not call this method from within a ZooKeeper watcher callback, as it might block for a longer
     * time and hence block the delivery of other events, including the Disconnected event.
     *
     */
    public static <T> T retryOperation(ZooKeeperOperation<T> operation)
            throws InterruptedException, KeeperException {
        // Disclaimer: this method was copied from ZooKeeper's lock recipe (class ProtocolSupport) and slightly altered
        int retryCount = -1;
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
