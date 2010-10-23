package org.lilyproject.indexer.engine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;

import java.util.Arrays;

// About the IndexLocker:
//
// To avoid multiple processes/threads concurrently indexing the same record, the convention is
// they are required to take an 'index lock' on the record.
//
// This lock is implemented using ZooKeeper. Given a single ZK quorum, this puts ultimately some
// limit on the number of locks that can be taken/released within a certain amount of time, and
// hence on the amount of records that can be indexed within that time, but we felt that at the
// moment this should be far from an issue. Also, the number of indexing processes is typically
// fairly limited.
//
// The IndexLocker does not take the common approach of having a lock path below which an ephemeral
// node is created by the party desiring to obtain the lock: this would require creating a non-ephemeral
// node for each record within ZK. Therefore, the lock is simply obtained by creating a node for
// the record within ZK. If this succeeds, you have the lock, if this fails because the node already
// exist, you have to wait a bit and retry.

public class IndexLocker {
    private ZooKeeperItf zk;
    private int waitBetweenTries = 20;
    private int maxWaitTime = 20000;

    private Log log = LogFactory.getLog(getClass());

    private static final String LOCK_PATH = "/lily/indexer/recordlock";        

    public IndexLocker(ZooKeeperItf zk) throws InterruptedException, KeeperException {
        this.zk = zk;
        ZkUtil.createPath(zk, LOCK_PATH);
    }

    public IndexLocker(ZooKeeperItf zk, int waitBetweenTries, int maxWaitTime) throws InterruptedException, KeeperException {
        this.zk = zk;
        this.waitBetweenTries = waitBetweenTries;
        this.maxWaitTime = maxWaitTime;
        ZkUtil.createPath(zk, LOCK_PATH);
    }

    /**
     * Obtain a lock for the given record. The lock is thread-based, i.e. it is re-entrant, obtaining
     * a lock for the same record twice from the same {ZK session, thread} will silently succeed.
     *
     * <p>If this method returns without failure, you obtained the lock
     *
     * @throws IndexLockTimeoutException if the lock could not be obtained within the given timeout.
     */
    public void lock(RecordId recordId) throws IndexLockException {
        if (zk.isCurrentThreadEventThread()) {
            throw new RuntimeException("IndexLocker should not be used from within the ZooKeeper event thread.");
        }

        try {
            long startTime = System.currentTimeMillis();
            final String lockPath = getPath(recordId);

            final byte[] data = Bytes.toBytes(Thread.currentThread().getId());

            while (true) {
                if (System.currentTimeMillis() - startTime > maxWaitTime) {
                    // we have been attempting long enough to get the lock, without success
                    throw new IndexLockTimeoutException("Failed to obtain an index lock for record " + recordId +
                            " within " + maxWaitTime + " ms.");
                }

                try {
                    zk.retryOperation(new ZooKeeperOperation<Object>() {
                        public Object execute() throws KeeperException, InterruptedException {
                            zk.create(lockPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            return null;
                        }
                    });
                    // We successfully created the node, hence we have the lock.
                    return;
                } catch (KeeperException.NodeExistsException e) {
                    // ignore, see next
                }

                // In case creating the node failed, it does not mean we do not have the lock: in case
                // of connection loss, we might not know if we actually succeeded creating the node, therefore
                // read the owner and thread id to check.
                boolean hasLock = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                    public Boolean execute() throws KeeperException, InterruptedException {
                        try {
                            Stat stat = new Stat();
                            byte[] currentData = zk.getData(lockPath, false, stat);
                            return (stat.getEphemeralOwner() == zk.getSessionId() && Arrays.equals(currentData, data));
                        } catch (KeeperException.NoNodeException e) {
                            return false;
                        }
                    }
                });

                if (hasLock) {
                    return;
                }

                Thread.sleep(waitBetweenTries);
            }
        } catch (Throwable throwable) {
            if (throwable instanceof IndexLockException)
                throw (IndexLockException)throwable;
            throw new IndexLockException("Error taking index lock on record " + recordId, throwable);
        }
    }

    public void unlock(final RecordId recordId) throws IndexLockException, InterruptedException,
            KeeperException {
        if (zk.isCurrentThreadEventThread()) {
            throw new RuntimeException("IndexLocker should not be used from within the ZooKeeper event thread.");
        }

        final String lockPath = getPath(recordId);

        // The below loop is because, even if our thread is interrupted, we still want to remove the lock.
        // The interruption might be because just one IndexUpdater is being shut down, rather than the
        // complete application, and hence session expiration will then not remove the lock.
        boolean tokenOk;
        boolean interrupted = false;
        while (true) {
            try {
                tokenOk = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                    public Boolean execute() throws KeeperException, InterruptedException {
                        Stat stat = new Stat();
                        byte[] data = zk.getData(lockPath, false, stat);

                        if (stat.getEphemeralOwner() == zk.getSessionId() && Bytes.toLong(data) == Thread.currentThread().getId()) {
                            zk.delete(lockPath, -1);
                            return true;
                        } else {
                            return false;
                        }
                    }
                });
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        if (!tokenOk) {
            throw new IndexLockException("You cannot remove the index lock for record " + recordId +
                    " because the token is incorrect.");
        }
    }

    public void unlockLogFailure(final RecordId recordId) {
        try {
            unlock(recordId);
        } catch (Throwable t) {
            log.error("Error releasing lock on record " + recordId, t);
        }
    }

    public boolean hasLock(final RecordId recordId) throws IndexLockException, InterruptedException,
            KeeperException {
        if (zk.isCurrentThreadEventThread()) {
            throw new RuntimeException("IndexLocker should not be used from within the ZooKeeper event thread.");
        }

        final String lockPath = getPath(recordId);

        return zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            public Boolean execute() throws KeeperException, InterruptedException {
                try {
                    Stat stat = new Stat();
                    byte[] data = zk.getData(lockPath, false, stat);
                    return stat.getEphemeralOwner() == zk.getSessionId() &&
                            Bytes.toLong(data) == Thread.currentThread().getId();
                } catch (KeeperException.NoNodeException e) {
                    return false;
                }

            }
        });
    }

    private String getPath(RecordId recordId) {
        return LOCK_PATH + "/" + recordId.toString();
    }

}
