package org.lilycms.indexer.engine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.lilycms.repository.api.RecordId;
import org.lilycms.util.zookeeper.ZkPathCreationException;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;
import org.lilycms.util.zookeeper.ZooKeeperOperation;

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

    public IndexLocker(ZooKeeperItf zk) throws ZkPathCreationException {
        this.zk = zk;
        ZkUtil.createPath(zk, LOCK_PATH);
    }

    public IndexLocker(ZooKeeperItf zk, int waitBetweenTries, int maxWaitTime) throws ZkPathCreationException {
        this.zk = zk;
        this.waitBetweenTries = waitBetweenTries;
        this.maxWaitTime = maxWaitTime;
        ZkUtil.createPath(zk, LOCK_PATH);
    }

    public IndexLock lock(RecordId recordId) throws IndexLockException {
        try {
            long startTime = System.currentTimeMillis();
            final String lockPath = getPath(recordId);

            // The token generation is simplistic but is not meant against hackers, only to protect against potential
            // programming errors.
            String randomToken = String.valueOf(Math.random() * 1000000);
            final byte[] token = Bytes.toBytes(randomToken);

            while (true) {
                if (System.currentTimeMillis() - startTime > maxWaitTime) {
                    // we have been attempting long enough to get the lock, without success
                    throw new IndexLockTimeoutException("Failed to obtain an index lock for record " + recordId +
                            " within " + maxWaitTime + " ms.");
                }

                try {
                    ZkUtil.retryOperationForever(new ZooKeeperOperation<Object>() {
                        public Object execute() throws KeeperException, InterruptedException {
                            zk.create(lockPath, token, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            return null;
                        }
                    });
                    break;
                } catch (KeeperException.NodeExistsException e) {
                    Thread.sleep(waitBetweenTries);
                }
            }

            return new IndexLock(recordId, token);
        } catch (Throwable throwable) {
            if (throwable instanceof IndexLockException)
                throw (IndexLockException)throwable;
            throw new IndexLockException("Error taking index lock on record " + recordId, throwable);
        }
    }

    public void unlock(final RecordId recordId, final IndexLock indexLock) throws IndexLockException, InterruptedException,
            KeeperException {
        final String lockPath = getPath(recordId);

        boolean tokenOk = ZkUtil.retryOperationForever(new ZooKeeperOperation<Boolean>() {
            public Boolean execute() throws KeeperException, InterruptedException {
                byte[] data = zk.getData(lockPath, false, null);

                if (indexLock.equals(new IndexLock(recordId, data))) {
                    zk.delete(lockPath, -1);
                    return true;
                } else {
                    return false;
                }

            }
        });

        if (!tokenOk) {
            throw new IndexLockException("You cannot remove the index lock for record " + recordId +
                    " because the token is incorrect.");
        }
    }

    public void unlockLogFailure(final RecordId recordId, final IndexLock token) {
        try {
            unlock(recordId, token);
        } catch (Throwable t) {
            log.error("Error releasing lock on record " + recordId, t);
        }
    }

    private String getPath(RecordId recordId) {
        return LOCK_PATH + "/" + recordId.toString();
    }

}
