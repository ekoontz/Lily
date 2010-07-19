/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.rowlog.impl.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;


public class RowLogEndToEndTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static RowLog rowLog;
    private static TestMessageConsumer consumer;
    private static RowLogShard shard;
    private static RowLogProcessor processor;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        Configuration configuration = HBASE_PROXY.getConf();
        HTableInterface rowTable = RowLogTableUtil.getRowTable(configuration);
        String zkConnectString = HBASE_PROXY.getZkConnectString();
        rowLog = new RowLogImpl("EndToEndRowLog", rowTable, RowLogTableUtil.PAYLOAD_COLUMN_FAMILY, RowLogTableUtil.EXECUTIONSTATE_COLUMN_FAMILY, 60000L, zkConnectString);
        shard = new RowLogShardImpl("EndToEndShard", configuration, rowLog, 100);
        consumer = new TestMessageConsumer(0);
        processor = new RowLogProcessorImpl(rowLog, shard, zkConnectString);
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testSingleMessage() throws Exception {
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        consumer.expectMessage(message);
        consumer.expectMessages(1);
        processor.start();
        if (!consumer.waitUntilMessagesConsumed(20000)) {
            fail("Messages not consumed within timeout");
        }
        processor.stop();
    }
    
    @Test
    public void testProcessorPublishesHost() throws Exception {
        processor.start();
        ZooKeeper zk = new ZooKeeper(HBASE_PROXY.getZkConnectString(), 50000, new Watcher() {
            
            public void process(WatchedEvent event) {
            }
        });
        byte[] data = zk.getData("/lily/rowLog/EndToEndRowLog/EndToEndShard", false, new Stat());
        assertNotNull(data);
        processor.stop();
        try {
            zk.getData("/lily/rowLog/EndToEndRowLog/EndToEndShard", false, new Stat());
            fail("The host in Zookeeper should have been deleted");
        } catch (KeeperException expected) {
        }
    }
    
    @Test
    public void testSingleMessageProcessorStartsFirst() throws Exception {
        processor.start();
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        consumer.expectMessage(message);
        consumer.expectMessages(1);
        if (!consumer.waitUntilMessagesConsumed(20000)) {
            fail("Messages not consumed within timeout");
        }
        processor.stop();
    }

    @Test
    public void testMultipleMessagesSameRow() throws Exception {
        RowLogMessage message; 
        for (int i = 0; i < 10; i++) {
            byte[] rowKey = Bytes.toBytes("row2");
            message = rowLog.putMessage(rowKey, null, null, null);
            consumer.expectMessage(message);
        }
        processor.start();
        if (!consumer.waitUntilMessagesConsumed(120000)) {
            fail("Messages not consumed within timeout");
        }
        processor.stop();
    }

    @Test
    public void testMultipleMessagesMultipleRows() throws Exception {
        RowLogMessage message; 
        consumer.expectMessages(25);
        for (long seqnr = 0L; seqnr < 5; seqnr++) {
            for (int rownr = 0; rownr < 5; rownr++) {
                byte[] data = Bytes.toBytes(rownr);
                data = Bytes.add(data, Bytes.toBytes(seqnr));
                message = rowLog.putMessage(Bytes.toBytes("row"+rownr), data, null, null);
                consumer.expectMessage(message);
            }
        }
        processor.start();
        if (!consumer.waitUntilMessagesConsumed(120000)) { // TODO avoid flipping tests
            fail("Messages not consumed within timeout");
        }
        processor.stop();
    }
    
    @Test
    public void testMultipleConsumers() throws Exception {
        TestMessageConsumer consumer2 = new TestMessageConsumer(1);
        rowLog.registerConsumer(consumer2);
        consumer.expectMessages(10);
        consumer2.expectMessages(10);
        RowLogMessage message; 
        for (long seqnr = 0L; seqnr < 2; seqnr++) {
            for (int rownr = 0; rownr < 5; rownr++) {
                byte[] data = Bytes.toBytes(rownr);
                data = Bytes.add(data, Bytes.toBytes(seqnr));
                message = rowLog.putMessage(Bytes.toBytes("row"+rownr), data, null, null);
                consumer.expectMessage(message);
                consumer2.expectMessage(message);
            }
        }
        processor.start();
        if (!consumer.waitUntilMessagesConsumed(120000)) { // TODO avoid flipping tests
            fail("Messages not consumed within timeout");
        }
        if (!consumer2.waitUntilMessagesConsumed(120000)) { // TODO avoid flipping tests
            fail("Messages not consumed within timeout");
        }
        processor.stop();
    }

    
    private static class TestMessageConsumer implements RowLogMessageConsumer {
        
        private List<RowLogMessage> expectedMessages = Collections.synchronizedList(new ArrayList<RowLogMessage>());
        private List<RowLogMessage> earlyMessages = Collections.synchronizedList(new ArrayList<RowLogMessage>());
        private int count = 0;
        private int numberOfMessagesToBeExpected = 0;
        private final int id;
        
        public TestMessageConsumer(int id) {
            this.id = id;
        }
        
        public void expectMessage(RowLogMessage message) {
            if (earlyMessages.contains(message)) {
                earlyMessages.remove(message);
                count++;
            } else {
                expectedMessages.add(message);
            }
        }
        
        public void expectMessages(int i) {
            this.numberOfMessagesToBeExpected = i;
        }

        public int getId() {
            return id;
        }
    
        public boolean processMessage(RowLogMessage message) {
            if (!expectedMessages.contains(message)) {
                earlyMessages.add(message);
                return true;
            } else {
                boolean removed;
                if (removed = expectedMessages.remove(message)) {
                    count++;
                } 
                return removed;
            }
        }
        
        public boolean waitUntilMessagesConsumed(long timeout) throws Exception {
            long waitUntil = System.currentTimeMillis() + timeout;
            while ((!expectedMessages.isEmpty() || (count < numberOfMessagesToBeExpected)) && System.currentTimeMillis() < waitUntil) {
                Thread.sleep(100);
            }
            return expectedMessages.isEmpty();
        }

    }
}
