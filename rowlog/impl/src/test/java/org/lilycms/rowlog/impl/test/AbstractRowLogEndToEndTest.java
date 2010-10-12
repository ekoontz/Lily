package org.lilycms.rowlog.impl.test;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.api.RowLogSubscription;
import org.lilycms.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

public abstract class AbstractRowLogEndToEndTest {
    protected final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    protected static RowLog rowLog;
    protected static RowLogShard shard;
    protected static RowLogProcessor processor;
    protected static RowLogConfigurationManagerImpl rowLogConfigurationManager;
    protected String subscriptionId = "Subscription1";
    protected ValidationMessageListener validationListener;
    private static Configuration configuration;
    protected static ZooKeeperItf zooKeeper;

    @Rule public TestName name = new TestName();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        configuration = HBASE_PROXY.getConf();
        HTableInterface rowTable = RowLogTableUtil.getRowTable(configuration);
        // Using a large ZooKeeper timeout, seems to help the build to succeed on Hudson (not sure if this is
        // the problem or the sympton, but HBase's Sleeper thread also reports it slept to long, so it appears
        // to be JVM-level).
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 120000);
        rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        rowLog = new RowLogImpl("EndToEndRowLog", rowTable, RowLogTableUtil.PAYLOAD_COLUMN_FAMILY,
                RowLogTableUtil.EXECUTIONSTATE_COLUMN_FAMILY, 60000L, true, rowLogConfigurationManager);
        shard = new RowLogShardImpl("EndToEndShard", configuration, rowLog, 100);
        rowLog.registerShard(shard);
        processor = new RowLogProcessorImpl(rowLog, rowLogConfigurationManager);
    }    
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        processor.stop();
        Closer.close(rowLogConfigurationManager);
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }
    
    @Test
    public void testSingleMessage() throws Exception {
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        validationListener.expectMessage(message);
        validationListener.expectMessages(1);
        processor.start();
        validationListener.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener.validate();
    }

    @Test
    public void testProcessorPublishesHost() throws Exception {
        Assert.assertTrue(rowLogConfigurationManager.getProcessorHost(rowLog.getId(), shard.getId()) == null);
        processor.start();
        assertNotNull(rowLogConfigurationManager.getProcessorHost(rowLog.getId(), shard.getId()));
        processor.stop();
        Assert.assertTrue(rowLogConfigurationManager.getProcessorHost(rowLog.getId(), shard.getId()) == null);
    }

    @Test
    public void testSingleMessageProcessorStartsFirst() throws Exception {
        processor.start();
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
        validationListener.expectMessage(message);
        validationListener.expectMessages(1);
        validationListener.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener.validate();
    }

    @Test
    public void testMultipleMessagesSameRow() throws Exception {
        RowLogMessage message;
        validationListener.expectMessages(10);
        for (int i = 0; i < 10; i++) {
            byte[] rowKey = Bytes.toBytes("row3");
            message = rowLog.putMessage(rowKey, null, "aPayload".getBytes(), null);
            validationListener.expectMessage(message);
        }
        processor.start();
        validationListener.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener.validate();
    }

    @Test
    public void testMultipleMessagesMultipleRows() throws Exception {
        RowLogMessage message;
        validationListener.expectMessages(25);
        for (long seqnr = 0L; seqnr < 5; seqnr++) {
            for (int rownr = 10; rownr < 15; rownr++) {
                byte[] data = Bytes.toBytes(rownr);
                data = Bytes.add(data, Bytes.toBytes(seqnr));
                message = rowLog.putMessage(Bytes.toBytes("row" + rownr), data, null, null);
                validationListener.expectMessage(message);
            }
        }
        processor.start();
        validationListener.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener.validate();
    }

    @Test
    public void testProblematicMessage() throws Exception {
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        validationListener.problematicMessages.add(message);
        validationListener.expectMessage(message, 3);
        validationListener.expectMessages(3);
        processor.start();
        validationListener.waitUntilMessagesConsumed(120000);
        processor.stop();
        Assert.assertTrue(rowLog.isProblematic(message, subscriptionId));
        validationListener.validate();
    }
    
    protected void waitForSubscription(String subscriptionId) throws InterruptedException {
        boolean subscriptionKnown = false;
        long waitUntil = System.currentTimeMillis() + 10000;
        while (!subscriptionKnown && System.currentTimeMillis() < waitUntil) {
            for (RowLogSubscription subscription : rowLog.getSubscriptions()) {
                if (subscriptionId.equals(subscription.getId())) {
                    subscriptionKnown = true;
                    break;
                }
            }
            Thread.sleep(10);
        }
        Assert.assertTrue("Subscription <" + subscriptionId +"> not known to rowlog within reasonable time <10s>", subscriptionKnown);
    }
}
