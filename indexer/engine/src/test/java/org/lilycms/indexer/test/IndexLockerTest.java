package org.lilycms.indexer.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.indexer.engine.IndexLock;
import org.lilycms.indexer.engine.IndexLockException;
import org.lilycms.indexer.engine.IndexLockTimeoutException;
import org.lilycms.indexer.engine.IndexLocker;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.net.NetUtils;
import org.lilycms.util.zookeeper.ZooKeeperImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexLockerTest {
    private static MiniZooKeeperCluster ZK_CLUSTER;
    private static File ZK_DIR;
    private static int ZK_CLIENT_PORT;
    private static ZooKeeperItf ZK;

    private static Log log = LogFactory.getLog(IndexLockerTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging(IndexLockerTest.class.getName());

        ZK_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "lily.indexerlocktest");
        ZK_CLIENT_PORT = NetUtils.getFreePort();

        ZK_CLUSTER = new MiniZooKeeperCluster();
        ZK_CLUSTER.setClientPort(ZK_CLIENT_PORT);
        ZK_CLUSTER.startup(ZK_DIR);

        ZK = new ZooKeeperImpl("localhost:" + ZK_CLIENT_PORT, 3000, new Watcher() {
            public void process(WatchedEvent event) {
            }
        });
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (ZK_CLUSTER != null) {
            ZK_CLUSTER.shutdown();
        }
    }

    @Test
    public void testUnlockRequiresCorrectLock() throws Exception {
        IndexLocker indexLocker = new IndexLocker(ZK);
        RecordId recordId1 = new IdGeneratorImpl().newRecordId();
        RecordId recordId2 = new IdGeneratorImpl().newRecordId();

        IndexLock lock1 = indexLocker.lock(recordId1);
        IndexLock lock2 = indexLocker.lock(recordId2);

        try {
            indexLocker.unlock(recordId1, lock2);
            fail("Expected exception");
        } catch (IndexLockException e) {
            // expected
        }

        indexLocker.unlock(recordId1, lock1);
        indexLocker.unlock(recordId2, lock2);
    }

    @Test
    public void testLockTimeout() throws Exception {
        int maxWaitTime = 500;
        IndexLocker indexLocker = new IndexLocker(ZK, 2, maxWaitTime);
        RecordId recordId = new IdGeneratorImpl().newRecordId();

        // take a lock and do not release it, another attempt to take a lock on the same record
        // should then fail after a certain timeout
        indexLocker.lock(recordId);

        long before = System.currentTimeMillis();
        try {
            indexLocker.lock(recordId);
            fail("expected exception");
        } catch (IndexLockTimeoutException e) {
            // expected
        }
        long after = System.currentTimeMillis();

        assertTrue(after - before > maxWaitTime);
        assertTrue(after - before < maxWaitTime + 200);
    }

    @Test
    public void testLockConcurrencyOnSameRecord() throws Exception {
        IndexLocker indexLocker = new IndexLocker(ZK);
        RecordId recordId = new IdGeneratorImpl().newRecordId();

        List<Info> infos = new ArrayList<Info>();

        for (int i = 0; i < 7; i++) {
            Info info = new Info();
            Thread thread = new Thread(new Locker(i + 1, info, indexLocker, recordId));
            info.thread = thread;
            infos.add(info);
        }

        for (Info info : infos) {
            info.thread.start();
        }

        for (Info info : infos) {
            info.thread.join();
        }

        for (Info info : infos) {
            if (info.throwable != null) {
                info.throwable.printStackTrace();
                fail("One of (un)lock operations failed, stacktrace should be visible in logging output.");
            }
        }

        // Sort by the time the lock was obtained
        Collections.sort(infos, new Comparator<Info>() {
            public int compare(Info o1, Info o2) {
                return (int)(o1.lockObtainTime - o2.lockObtainTime);
            }
        });

        // Check that no 2 threads had the lock at the same time
        for (int i = 1; i < infos.size(); i++) {
            assertTrue(infos.get(i).lockObtainTime > infos.get(i - 1).lockReleaseTime);
        }        
    }

    private static class Locker implements Runnable {
        private int number;
        private Info info;
        private IndexLocker indexLocker;
        private RecordId recordId;

        public Locker(int number, Info info, IndexLocker indexLocker, RecordId recordId) {
            this.number = number;
            this.info = info;
            this.indexLocker = indexLocker;
            this.recordId = recordId;
        }

        public void run() {
            try {
                log.info("Thread " + number + " waiting to obtain lock");
                IndexLock lock = indexLocker.lock(recordId);
                log.debug("Thread " + number + " obtained lock");
                info.lockObtainTime = System.currentTimeMillis();
                Thread.sleep((long)Math.floor(Math.random() * 1000));
                info.lockReleaseTime = System.currentTimeMillis();
                indexLocker.unlock(recordId, lock);
                log.debug("Thread " + number + " released lock");
            } catch (Throwable t) {
                log.error("Thread " + number + " failed", t);
                info.throwable = t;
            }
        }
    }

    public static class Info {
        Thread thread;
        long lockObtainTime;
        long lockReleaseTime;
        Throwable throwable;
    }

}
