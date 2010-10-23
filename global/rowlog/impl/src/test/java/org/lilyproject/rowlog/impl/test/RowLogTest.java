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
package org.lilyproject.rowlog.impl.test;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogMessageImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class RowLogTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static RowLogConfigurationManager configurationManager;
    private static IMocksControl control;
    private static RowLog rowLog;
    private static byte[] payloadColumnFamily = RowLogTableUtil.PAYLOAD_COLUMN_FAMILY;
    private static byte[] rowLogColumnFamily = RowLogTableUtil.EXECUTIONSTATE_COLUMN_FAMILY;
    private static HTableInterface rowTable;
    private static String subscriptionId1 = "SubscriptionId";
    private static String RowLogId = "RowLogTest";
    private static ZooKeeperItf zooKeeper;
    private RowLogShard shard;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);
        configurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        configurationManager.addSubscription(RowLogId, subscriptionId1, Type.VM, 3, 1);
        control = createControl();
        rowTable = RowLogTableUtil.getRowTable(HBASE_PROXY.getConf());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(zooKeeper);
        Closer.close(configurationManager);
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
        rowLog = new RowLogImpl(RowLogId, rowTable, payloadColumnFamily, rowLogColumnFamily, 60000L, true, configurationManager);
        shard = control.createMock(RowLogShard.class);
        shard.getId();
        expectLastCall().andReturn("ShardId").anyTimes();
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }
    
    @Test
    public void testPutMessage() throws Exception {
        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        control.replay();
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row1");
        RowLogMessage message = rowLog.putMessage(rowKey, null, null, null);
        List<RowLogMessage> messages = rowLog.getMessages(rowKey);
        assertEquals(1, messages.size());
        assertEquals(message, messages.get(0));
        control.verify();
    }
    
    @Test
    public void testMultipleMessages() throws Exception {
        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        expectLastCall().times(3);
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId1));
        
        control.replay();
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row2");
        RowLogMessage message1 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message2 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message3 = rowLog.putMessage(rowKey, null, null, null);
        rowLog.messageDone(message2, subscriptionId1, null);
        
        List<RowLogMessage> messages = rowLog.getMessages(rowKey, subscriptionId1);
        assertEquals(2, messages.size());
        assertEquals(message1, messages.get(0));
        assertEquals(message3, messages.get(1));
        control.verify();
    }
    
    @Test
    public void testNoShardsRegistered() throws Exception {

        control.replay();
        try {
            rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
            fail("Expected a MessageQueueException since no shards are registered");
        } catch (RowLogException expected) {
        }
        
        RowLogMessage message = new RowLogMessageImpl(Bytes.toBytes("id"), Bytes.toBytes("row1"), 0L, null, rowLog);
        try {
            rowLog.messageDone(message , subscriptionId1, null);
            fail("Expected a MessageQueueException since no shards are registered");
        } catch (RowLogException expected) {
        }
        // Cleanup
        
        control.verify();
    }
    
    @Test
    public void testMessageConsumed() throws Exception {

        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId1));
        
        control.replay();
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);

        byte[] lock = rowLog.lockMessage(message, subscriptionId1);
        rowLog.messageDone(message, subscriptionId1, lock);
        assertFalse(rowLog.isMessageLocked(message, subscriptionId1));
        control.verify();
    }
    
    @Test
    public void testLockMessage() throws Exception {
        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        
        control.replay();
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        
        assertNotNull(rowLog.lockMessage(message, subscriptionId1));
        assertTrue(rowLog.isMessageLocked(message, subscriptionId1));
        assertNull(rowLog.lockMessage(message, subscriptionId1));
        control.verify();
    }
    
    @Test
    public void testUnlockMessage() throws Exception {
        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        
        control.replay();
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
        
        byte[] lock = rowLog.lockMessage(message, subscriptionId1);
        assertNotNull(lock);
        assertTrue(rowLog.unlockMessage(message, subscriptionId1, true, lock));
        assertFalse(rowLog.isMessageLocked(message, subscriptionId1));
        byte[] lock2 = rowLog.lockMessage(message, subscriptionId1);
        assertNotNull(lock2);
        control.verify();
        //Cleanup 
        rowLog.unlockMessage(message, subscriptionId1, true, lock2);
    }
    
    @Test
    public void testLockTimeout() throws Exception {
        RowLog rowLog = new RowLogImpl(RowLogId, rowTable, payloadColumnFamily, rowLogColumnFamily, 1L, true, configurationManager);

        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        
        control.replay();
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
        
        byte[] lock = rowLog.lockMessage(message, subscriptionId1);
        assertNotNull(lock);
        Thread.sleep(10L);
        assertFalse(rowLog.isMessageLocked(message, subscriptionId1));
        byte[] lock2 = rowLog.lockMessage(message, subscriptionId1);
        assertNotNull(lock2);
        
        assertFalse(rowLog.unlockMessage(message, subscriptionId1, true, lock));
        control.verify();
        //Cleanup
        rowLog.unlockMessage(message, subscriptionId1, true, lock2);
    }
    
    @Test
    public void testLockingMultipleConsumers() throws Exception {
        String subscriptionId2 = "subscriptionId2";
                
        RowLogConfigurationManagerImpl configurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        configurationManager.addSubscription(RowLogId, subscriptionId2, Type.Netty, 3, 2);
        long waitUntil = System.currentTimeMillis() + 10000;
        while (waitUntil > System.currentTimeMillis()) {
            if (rowLog.getSubscriptions().size() == 2)
                break;
        }
        assertEquals(2L, rowLog.getSubscriptions().size());
        
        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId2));
        control.replay();

        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row2");
        RowLogMessage message = rowLog.putMessage(rowKey, null, null, null);
        
        byte[] lock = rowLog.lockMessage(message, subscriptionId1);
        assertNotNull(lock);
        assertFalse(rowLog.isMessageLocked(message, subscriptionId2));
        assertTrue(rowLog.unlockMessage(message, subscriptionId1, true, lock));
        assertFalse(rowLog.isMessageLocked(message, subscriptionId1));
        
        byte[] lock2 = rowLog.lockMessage(message, subscriptionId2);
        assertNotNull(lock2);
        rowLog.messageDone(message, subscriptionId2, lock2);
        assertFalse(rowLog.isMessageLocked(message, subscriptionId2));
        
        control.verify();
        //Cleanup 
        rowLog.unlockMessage(message, subscriptionId2, true, lock2);
        configurationManager.removeSubscription(RowLogId, subscriptionId2);

        waitUntil = System.currentTimeMillis() + 10000;
        while (waitUntil > System.currentTimeMillis()) {
            if (rowLog.getSubscriptions().size() == 1)
                break;
        }
        assertEquals(1L, rowLog.getSubscriptions().size());
    }
    
    @Test
    public void testgetMessages() throws Exception {
        String subscriptionId3 = "subscriptionId2";
        
        RowLogConfigurationManagerImpl configurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        configurationManager.addSubscription(RowLogId, subscriptionId3, Type.VM, 5, 3);
        
        long waitUntil = System.currentTimeMillis() + 10000;
        while (waitUntil > System.currentTimeMillis()) {
            if (rowLog.getSubscriptions().size() > 1)
                break;
        }
        
        assertEquals(2, rowLog.getSubscriptions().size());
        
        shard.putMessage(isA(RowLogMessage.class), eq(rowLog.getSubscriptions()));
        expectLastCall().times(2);
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId1));
        shard.removeMessage(isA(RowLogMessage.class), eq(subscriptionId3));

        control.replay();

        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row3");
        RowLogMessage message1 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message2 = rowLog.putMessage(rowKey, null, null, null);

        byte[] lock = rowLog.lockMessage(message1, subscriptionId1);
        rowLog.messageDone(message1, subscriptionId1, lock);
        lock = rowLog.lockMessage(message2, subscriptionId3);
        rowLog.messageDone(message2, subscriptionId3, lock);
        
        List<RowLogMessage> messages;
        messages = rowLog.getMessages(rowKey);
        assertEquals(2, messages.size());
        
        messages = rowLog.getMessages(rowKey, subscriptionId1);
        assertEquals(1, messages.size());
        assertEquals(message2, messages.get(0));
        
        messages = rowLog.getMessages(rowKey, subscriptionId3);
        assertEquals(1, messages.size());
        assertEquals(message1, messages.get(0));
        
        messages = rowLog.getMessages(rowKey, subscriptionId1, subscriptionId3);
        assertEquals(2, messages.size());
        
        control.verify();
        configurationManager.removeSubscription(RowLogId, subscriptionId3);
    }
}
