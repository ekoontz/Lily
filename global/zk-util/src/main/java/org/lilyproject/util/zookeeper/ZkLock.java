package org.lilyproject.util.zookeeper;

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
        if (zk.isCurrentThreadEventThread()) {
            throw new RuntimeException("ZkLock should not be used from within the ZooKeeper event thread.");
        }

        try {
            final long threadId = Thread.currentThread().getId();

            // Quote from ZK lock recipe:
            //    1. Call create( ) with a pathname of "_locknode_/lock-" and the sequence and ephemeral flags set.
            zk.retryOperation(new ZooKeeperOperation<String>() {
                public String execute() throws KeeperException, InterruptedException {
                    return zk.create(lockPath + "/lock-" + threadId + "-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL_SEQUENTIAL);
                }
            });

            while (true) {
                // Quote from ZK lock recipe:
                //    2. Call getChildren( ) on the lock node without setting the watch flag (this is important to avoid
                //       the herd effect).
                List<ZkLockNode> children = parseChildren(zk.retryOperation(new ZooKeeperOperation<List<String>>() {
                    public List<String> execute() throws KeeperException, InterruptedException {
                        return zk.getChildren(lockPath, null);
                    }
                }));

                ZkLockNode myLockNode = null;
                String myLockName = null;
                String myLockPath = null;
                for (ZkLockNode child : children) {
                    // if the child has the same thread id and session id as us, then it is our lock
                    if (child.getThreadId() == threadId) {
                        final String childPath = lockPath + "/" + child.getName();
                        Stat stat = zk.retryOperation(new ZooKeeperOperation<Stat>() {
                            public Stat execute() throws KeeperException, InterruptedException {
                                return zk.exists(childPath, false);
                            }
                        });
                        if (stat != null && stat.getEphemeralOwner() == zk.getSessionId()) {
                            if (myLockName != null) {
                                // We have found another lock node which belongs to us.
                                // This means that the lock creation above was executed twice, which can occur
                                // in case of connection loss. Delete this node to avoid that otherwise it would
                                // never be released.
                                zk.retryOperation(new ZooKeeperOperation<Object>() {
                                    public Object execute() throws KeeperException, InterruptedException {
                                        try {
                                            zk.delete(childPath, -1);
                                        } catch (KeeperException.NoNodeException e) {
                                            // ignore
                                        }
                                        return null;
                                    }
                                });
                            } else {
                                myLockNode = child;
                                myLockName = child.getName();
                                myLockPath = childPath;
                            }
                        }
                    }
                }

                if (myLockName == null) {
                    throw new ZkLockException("Unexpected problem: did not find our lock node.");
                }

                // Idea to use SortedSets seen in a ZK recipe
                SortedSet<ZkLockNode> sortedChildren = new TreeSet<ZkLockNode>(children);
                SortedSet<ZkLockNode> lowerThanMe = sortedChildren.headSet(myLockNode);

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

                final String pathToWatch = lockPath + "/" + lowerThanMe.last().name;
                final Object condition = new Object();
                final MyWatcher watcher = new MyWatcher(pathToWatch, condition);

                Stat stat = zk.retryOperation(new ZooKeeperOperation<Stat>() {
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
        if (zk.isCurrentThreadEventThread()) {
            throw new RuntimeException("ZkLock should not be used from within the ZooKeeper event thread.");
        }

        try {
            zk.retryOperation(new ZooKeeperOperation<Object>() {
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
        if (zk.isCurrentThreadEventThread()) {
            throw new RuntimeException("ZkLock should not be used from within the ZooKeeper event thread.");
        }

        try {
            int lastSlashPos = lockId.lastIndexOf('/');
            final String lockPath = lockId.substring(0, lastSlashPos);
            String lockName = lockId.substring(lastSlashPos + 1);

            List<String> children = zk.retryOperation(new ZooKeeperOperation<List<String>>() {
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

    private static List<ZkLockNode> parseChildren(List<String> children) {
        List<ZkLockNode> result = new ArrayList<ZkLockNode>(children.size());
        for (String child : children) {
            result.add(new ZkLockNode(child));
        }
        return result;
    }

    private static class ZkLockNode implements Comparable<ZkLockNode> {
        private long threadId;
        private int seqNr;
        private String name;

        public ZkLockNode(String name) {
            this.name = name;

            int dash1Pos = name.indexOf('-');
            int dash2Pos = name.indexOf('-', dash1Pos + 1);

            this.threadId = Long.parseLong(name.substring(dash1Pos + 1, dash2Pos));
            this.seqNr = Integer.parseInt(name.substring(dash2Pos + 1));
        }

        public long getThreadId() {
            return threadId;
        }

        public int getSeqNr() {
            return seqNr;
        }

        public String getName() {
            return name;
        }

        public int compareTo(ZkLockNode o) {
            return seqNr - o.seqNr;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ZkLockNode other = (ZkLockNode)obj;

            return other.threadId == threadId && other.seqNr == seqNr;
        }
    }
}
