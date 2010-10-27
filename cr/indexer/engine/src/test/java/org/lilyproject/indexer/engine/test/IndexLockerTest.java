package org.lilyproject.indexer.engine.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.indexer.engine.IndexLockTimeoutException;
import org.lilyproject.indexer.engine.IndexLocker;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.net.NetUtils;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertNull;
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

        ZK = ZkUtil.connect("localhost:" + ZK_CLIENT_PORT, 3000);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(ZK);
        if (ZK_CLUSTER != null) {
            ZK_CLUSTER.shutdown();
        }
    }

    @Test
    public void testObtainAndReleaseLock() throws Exception {
        IndexLocker indexLocker = new IndexLocker(ZK);
        RecordId recordId1 = new IdGeneratorImpl().newRecordId();
        RecordId recordId2 = new IdGeneratorImpl().newRecordId();

        // working with two records just to illustrate that one thread can take
        // a lock on more than one record at the same time.

        indexLocker.lock(recordId1);
        indexLocker.lock(recordId2);

        indexLocker.unlock(recordId1);
        indexLocker.unlock(recordId2);
    }

    @Test
    public void testLockTimeout() throws Exception {
        int maxWaitTime = 500;
        final IndexLocker indexLocker = new IndexLocker(ZK, 2, maxWaitTime);
        final RecordId recordId = new IdGeneratorImpl().newRecordId();

        // take a lock and do not release it, another attempt to take a lock on the same record
        // should then fail after a certain timeout
        indexLocker.lock(recordId);

        // since locks are re-entrant, we need to obtain the lock a second time on a different thread
        final Variable<Long> before = new Variable<Long>();
        final Variable<Long> after = new Variable<Long>();
        final Variable<Throwable> throwable = new Variable<Throwable>();

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    before.value = System.currentTimeMillis();
                    try {
                        indexLocker.lock(recordId);
                        fail("expected exception");
                    } catch (IndexLockTimeoutException e) {
                        // expected
                    }
                    after.value = System.currentTimeMillis();
                } catch (Throwable t) {
                    throwable.value = t;
                }
            }
        };

        assertNull(throwable.value);

        Thread thread = new Thread(runnable);
        thread.start();
        thread.join();

        assertTrue(after.value - before.value > maxWaitTime);
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
            assertTrue(infos.get(i).lockObtainTime >= infos.get(i - 1).lockReleaseTime);
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
                indexLocker.lock(recordId);
                log.debug("Thread " + number + " obtained lock");
                info.lockObtainTime = System.currentTimeMillis();
                Thread.sleep((long)Math.floor(Math.random() * 1000));
                info.lockReleaseTime = System.currentTimeMillis();
                indexLocker.unlock(recordId);
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

    public static class Variable<T> {
        public T value;
    }
}
