package org.lilycms.rowlog.impl.test;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

public abstract class AbstractRowLogEndToEndTest {
    protected final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    protected static RowLog rowLog;
    protected static TestMessageConsumer consumer;
    protected static RowLogShard shard;
    protected static RowLogProcessor processor;
    protected static String zkConnectString;
    protected static RowLogConfigurationManagerImpl rowLogConfigurationManager;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        Configuration configuration = HBASE_PROXY.getConf();
        HTableInterface rowTable = RowLogTableUtil.getRowTable(configuration);
        zkConnectString = HBASE_PROXY.getZkConnectString();
        rowLog = new RowLogImpl("EndToEndRowLog", rowTable, RowLogTableUtil.PAYLOAD_COLUMN_FAMILY,
                RowLogTableUtil.EXECUTIONSTATE_COLUMN_FAMILY, 60000L, HBASE_PROXY.getConf());
        shard = new RowLogShardImpl("EndToEndShard", configuration, rowLog, 100);
        processor = new RowLogProcessorImpl(rowLog, shard, HBASE_PROXY.getConf());
        rowLog.registerShard(shard);
    }    
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
      rowLogConfigurationManager.stop();
      processor.stop();
      HBASE_PROXY.stop();
    }
    
    @Test
    public void testSingleMessage() throws Exception {
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        consumer.expectMessage(message);
        consumer.expectMessages(1);
        processor.start();
        consumer.waitUntilMessagesConsumed(120000);
        processor.stop();
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
        consumer.expectMessage(message);
        consumer.expectMessages(1);
        consumer.waitUntilMessagesConsumed(120000);
        processor.stop();
    }

    @Test
    public void testMultipleMessagesSameRow() throws Exception {
        RowLogMessage message;
        consumer.expectMessages(10);
        for (int i = 0; i < 10; i++) {
            byte[] rowKey = Bytes.toBytes("row3");
            message = rowLog.putMessage(rowKey, null, "aPayload".getBytes(), null);
            consumer.expectMessage(message);
        }
        processor.start();
        consumer.waitUntilMessagesConsumed(120000);
        processor.stop();
    }

    @Test
    public void testMultipleMessagesMultipleRows() throws Exception {
        RowLogMessage message;
        consumer.expectMessages(25);
        for (long seqnr = 0L; seqnr < 5; seqnr++) {
            for (int rownr = 10; rownr < 15; rownr++) {
                byte[] data = Bytes.toBytes(rownr);
                data = Bytes.add(data, Bytes.toBytes(seqnr));
                message = rowLog.putMessage(Bytes.toBytes("row" + rownr), data, null, null);
                consumer.expectMessage(message);
            }
        }
        processor.start();
        consumer.waitUntilMessagesConsumed(120000);
        processor.stop();
    }

    @Test
    public void testProblematicMessage() throws Exception {
//        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
//        consumer.problematicMessages.add(message);
//        consumer.expectMessage(message, 3);
//        consumer.expectMessages(3);
//        processor.start();
//        consumer.waitUntilMessagesConsumed(120000);
//        processor.stop();
//        Assert.assertTrue(rowLog.isProblematic(message, consumer.getId()));
    }

    protected class TestMessageConsumer implements RowLogMessageListener {

        private Map<RowLogMessage, Integer> expectedMessages = new HashMap<RowLogMessage, Integer>();
        private Map<RowLogMessage, Integer> earlyMessages = new HashMap<RowLogMessage, Integer>();
        public List<RowLogMessage> problematicMessages = new ArrayList<RowLogMessage>();
        private int count = 0;
        private int numberOfMessagesToBeExpected = 0;
        private final int id;
        public int maxTries = 3;

        public TestMessageConsumer(int id) {
            this.id = id;
        }

        public int getMaxTries() {
            return maxTries;
        }

        public void expectMessage(RowLogMessage message) throws Exception {
            expectMessage(message, 1);
        }

        public void expectMessage(RowLogMessage message, int times) throws Exception {
            if (earlyMessages.containsKey(message)) {
                int timesEarlyReceived = earlyMessages.get(message);
                count = count + timesEarlyReceived;
                int remainingTimes = times - timesEarlyReceived;
                if (remainingTimes < 0)
                    throw new Exception("Recieved message <" + message + "> more than expected");
                earlyMessages.remove(message);
                if (remainingTimes > 0) {
                    expectedMessages.put(message, remainingTimes);
                }
            } else {
                expectedMessages.put(message, times);
            }
        }

        public void expectMessages(int i) {
            this.numberOfMessagesToBeExpected = i;
        }

        public int getId() {
            return id;
        }

        public boolean processMessage(RowLogMessage message) {
            boolean result;
            if (!expectedMessages.containsKey(message)) {
                if (earlyMessages.containsKey(message)) {
                    earlyMessages.put(message, earlyMessages.get(message) + 1);
                } else {
                    earlyMessages.put(message, 1);
                }
                result = (!problematicMessages.contains(message));
            } else {
                count++;
                int timesRemaining = expectedMessages.get(message);
                if (timesRemaining == 1) {
                    expectedMessages.remove(message);
                    result = (!problematicMessages.contains(message));
                } else {
                    expectedMessages.put(message, timesRemaining - 1);
                    result = false;
                }
            }
            return result;
        }

        public void waitUntilMessagesConsumed(long timeout) throws Exception {
            long waitUntil = System.currentTimeMillis() + timeout;
            while ((!expectedMessages.isEmpty() || (count < numberOfMessagesToBeExpected))
                    && System.currentTimeMillis() < waitUntil) {
                Thread.sleep(100);
            }
        }

        public void validate() throws Exception {
            Assert.assertFalse("Received less messages <"+count+"> than expected <"+numberOfMessagesToBeExpected+">", (count < numberOfMessagesToBeExpected));
            Assert.assertFalse("Received more messages <"+count+"> than expected <"+numberOfMessagesToBeExpected+">", (count > numberOfMessagesToBeExpected));
            Assert.assertTrue("EarlyMessages list is not empty <"+earlyMessages.keySet()+">", earlyMessages.isEmpty());
            Assert.assertTrue("Expected messages not processed within timeout", expectedMessages.isEmpty());
        }

    }
}
