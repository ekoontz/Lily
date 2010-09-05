package org.lilycms.util.zookeeper.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.zookeeper.ZkLock;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;


import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ZkLockTest {
    private static MiniZooKeeperCluster ZK_CLUSTER;
    private static File ZK_DIR;
    private static int ZK_CLIENT_PORT;
    private static ZooKeeperItf ZK;

    private static Log log = LogFactory.getLog(ZkLockTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilycms.util.zookeeper");

        ZK_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "lily.zklocktest");
        ZK_CLIENT_PORT = determineAvailablePort();

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
    public void testObtainReleaseLock() throws Exception {
        ZkUtil.createPath(ZK, "/lily/test/zklockA");
        String obtainedLock = ZkLock.lock(ZK, "/lily/test/zklockA");
        ZkLock.unlock(ZK, obtainedLock);
    }

    /**
     * Tests the following: first user takes lock, then second user tries to take lock before first has released
     * it. Then first releases it, after this second user gets the lock.
     * @throws Exception
     */
    @Test
    public void testTwoUsersForSameLock() throws Exception {
        final String lockPath = "/lily/test/zklockB";
        ZkUtil.createPath(ZK, lockPath);

        log.debug("Request first lock.");
        String obtainedLock1 = ZkLock.lock(ZK, lockPath);
        log.debug("First lock obtained.");

        final Variable<String> obtainedLock2 = new Variable<String>();
        final Variable<Long> obtainTime = new Variable<Long>();
        final Variable<Throwable> throwable = new Variable<Throwable>();

        // The following will block
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    log.debug("Request second lock.");
                    obtainedLock2.value = ZkLock.lock(ZK, lockPath);
                    obtainTime.value = System.currentTimeMillis();
                    log.debug("Second lock obtained.");
                } catch (Throwable t) {
                    throwable.value = t;
                }
            }
        });
        t.start();

        // Wait more then enough time for the second lock to be obtained (thread needs to start)
        Thread.sleep(2000);

        long releaseTime = System.currentTimeMillis();
        log.debug("Will now release first lock.");
        ZkLock.unlock(ZK, obtainedLock1);
        log.debug("First lock released.");

        // wait for thread to end
        t.join();

        if (throwable.value != null) {
            throwable.value.printStackTrace();
            fail("Failure in second lock thread.");
        }

        // It should only be after release of the first lock that the second lock can be obtained.
        log.debug("First lock released at " + releaseTime + ", second lock obtained at " + obtainTime.value);
        assertTrue(releaseTime < obtainTime.value);

        // remove second lock
        log.debug("Will now release second lock.");
        ZkLock.unlock(ZK, obtainedLock2.value);
        log.debug("Second lock released.");
    }

    public static class Variable<T> {
        public T value;
    }

    private static int determineAvailablePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Error determining an available port", e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing ServerSocket used to detect an available port.", e);
                }
            }
        }
    }
}
