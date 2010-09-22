package org.lilycms.util.zookeeper;

import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;

/**
 * Implements a ZooKeeper-based lock following ZooKeeper's lock recipe.
 *
 * <p>The recipe is also in the paper on ZooKeeper, in more compact (and clearer) form:
 *
 * <pre>
 * Lock
 * 1 n = create(l + “/lock-”, EPHEMERAL|SEQUENTIAL)
 * 2 C = getChildren(l, false)
 * 3 if n is lowest znode in C, exit
 * 4 p = znode in C ordered just before n
 * 5 if exists(p, true) wait for watch event
 * 6 goto 2
 *
 * Unlock
 * 1 delete(n)
 * </pre>
 *
 * <p><b>Important usage note:</b> do not take ZKLock's in ZooKeeper's Watcher.process()
 * callback, unless the ZooKeeper handle used to take the lock is different from the one
 * of that called the Watcher. This is because ZkLock might need to wait for an event
 * too (if the lock is currently held by someone else), and since all ZK events to all
 * watchers are dispatched by one thread, it would hang forever. This limitation is not
 * very important, since long lasting actions (which waiting for a lock could be) should
 * never be done in Watcher callbacks.
 *
 */
public class ZkLock {

    private ZkLock() {
    }

    /**
     * Try to get a lock, waits until this succeeds.
     *
     * @param lockPath path in ZooKeeper below which the ephemeral lock nodes will be created. This path should
     *                 exist prior to calling this method.
     *
     * @return a string identifying this lock, needs to be supplied to {@link ZkLock#unlock}.
     */
    public static String lock(final ZooKeeperItf zk, final String lockPath) throws ZkLockException {
        try {
            // Quote from ZK lock recipe:
            //    1. Call create( ) with a pathname of "_locknode_/lock-" and the sequence and ephemeral flags set.
            String myLockPath = ZkUtil.retryOperationForever(new ZooKeeperOperation<String>() {
                public String execute() throws KeeperException, InterruptedException {
                    return zk.create(lockPath + "/lock-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL_SEQUENTIAL);
                }
            });
            String myLockName = myLockPath.substring(lockPath.length() + 1);

            while (true) {
                // Quote from ZK lock recipe:
                //    2. Call getChildren( ) on the lock node without setting the watch flag (this is important to avoid
                //       the herd effect).
                List<String> children = ZkUtil.retryOperationForever(new ZooKeeperOperation<List<String>>() {
                    public List<String> execute() throws KeeperException, InterruptedException {
                        return zk.getChildren(lockPath, null);
                    }
                });

                // Idea to use SortedSets seen in a ZK recipe
                SortedSet<String> sortedChildren = new TreeSet<String>(children);
                if (!sortedChildren.contains(myLockName)) {
                    throw new ZkLockException("Our lock does not occur in list of locks. Lock: " + myLockPath);
                }
                SortedSet<String> lowerThanMe = sortedChildren.headSet(myLockName);

                // Quote from ZK lock recipe:
                //    3. If the pathname created in step 1 has the lowest sequence number suffix, the client has the lock
                //       and the client exits the protocol.
                if (lowerThanMe.isEmpty()) {
                    // We have the lock
                    return myLockPath;
                }

                // Quote from ZK lock recipe:
                //    4. The client calls exists( ) with the watch flag set on the path in the lock directory with the
                //       next lowest sequence number.

                final String pathToWatch = lockPath + "/" + lowerThanMe.last();
                final Object condition = new Object();
                final MyWatcher watcher = new MyWatcher(pathToWatch, condition);

                Stat stat = ZkUtil.retryOperationForever(new ZooKeeperOperation<Stat>() {
                    public Stat execute() throws KeeperException, InterruptedException {
                        return zk.exists(pathToWatch, watcher);
                    }
                });

                if (stat == null) {
                    // Quote from ZK lock recipe:
                    //    5a. if exists( ) returns false, go to step 2.

                    // This means (I think) that the lock was removed (explicitly or through session expiration) between
                    // the moment we queried for the children and the moment we called exists()

                    // If the node does not exist, the watcher will still be left, does not seem so tidy, hopefully
                    // this situation does not occur often.

                    // Let's log it to keep an eye on it
                    LogFactory.getLog(ZkLock.class).warn("Next lower lock node does not exist: " + pathToWatch);
                } else {
                    // Quote from ZK lock recipe:
                    //    5b. Otherwise, wait for a notification for the pathname from the previous step before going
                    //        to step 2.
                    synchronized (condition) {
                        while (!watcher.gotNotified) {
                            condition.wait();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            throw new ZkLockException("Error obtaining lock, path: " + lockPath, t);
        }
    }

    /**
     * Releases a lock. Calls {@link #unlock(ZooKeeperItf, String, boolean) unlock(zk, lockId, false)}.
     */
    public static void unlock(final ZooKeeperItf zk, final String lockId) throws ZkLockException {
        unlock(zk, lockId, false);
    }

    /**
     * Releases a lock.
     *
     * @param lockId the string returned by {@link ZkLock#lock}.
     * @param ignoreMissing if true, do not throw an exception if the lock does not exist
     */
    public static void unlock(final ZooKeeperItf zk, final String lockId, boolean ignoreMissing) throws ZkLockException {
        try {
            ZkUtil.retryOperationForever(new ZooKeeperOperation<Object>() {
                public Object execute() throws KeeperException, InterruptedException {
                    zk.delete(lockId, -1);
                    return null;
                }
            });
        } catch (KeeperException.NoNodeException e) {
            if (!ignoreMissing) {
                throw new ZkLockException("Error releasing lock: the lock does not exist. Path: " + lockId, e);
            }
        } catch (Throwable t) {
            throw new ZkLockException("Error releasing lock, path: " + lockId, t);
        }
    }

    /**
     * Verifies that the specified lockId is the owner of the lock.
     */
    public static boolean ownsLock(final ZooKeeperItf zk, final String lockId) throws ZkLockException {
        try {
            int lastSlashPos = lockId.lastIndexOf('/');
            final String lockPath = lockId.substring(0, lastSlashPos);
            String lockName = lockId.substring(lastSlashPos + 1);

            List<String> children = ZkUtil.retryOperationForever(new ZooKeeperOperation<List<String>>() {
                public List<String> execute() throws KeeperException, InterruptedException {
                    return zk.getChildren(lockPath, null);
                }
            });

            if (children.isEmpty())
                return false;

            SortedSet<String> sortedChildren = new TreeSet<String>(children);

            return sortedChildren.first().equals(lockName);
        } catch (Throwable t) {
            throw new ZkLockException("Error checking lock, path: " + lockId, t);
        }
    }

    private static class MyWatcher implements Watcher {
        private String path;
        private final Object condition;
        private volatile boolean gotNotified;

        public MyWatcher(String path, Object condition) {
            this.path = path;
            this.condition = condition;
        }

        public void process(WatchedEvent event) {
            if (path.equals(event.getPath())) {
                synchronized (condition) {
                    gotNotified = true;
                    condition.notifyAll();
                }
            }
        }
    }
}
