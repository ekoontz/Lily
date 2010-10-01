package org.lilycms.rowlog.impl.test;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.zookeeper.StateWatchingZooKeeper;
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
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        configuration = HBASE_PROXY.getConf();
        HTableInterface rowTable = RowLogTableUtil.getRowTable(configuration);
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);
        rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        rowLog = new RowLogImpl("EndToEndRowLog", rowTable, RowLogTableUtil.PAYLOAD_COLUMN_FAMILY,
                RowLogTableUtil.EXECUTIONSTATE_COLUMN_FAMILY, 60000L, true, zooKeeper);
        shard = new RowLogShardImpl("EndToEndShard", configuration, rowLog, 100);
        rowLog.registerShard(shard);
        processor = new RowLogProcessorImpl(rowLog, zooKeeper);
    }    
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
      processor.stop();
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
}
