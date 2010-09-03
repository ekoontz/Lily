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

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogMessageImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

public class RowLogTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static IMocksControl control;
    private static RowLog rowLog;
    private static byte[] payloadColumnFamily = RowLogTableUtil.PAYLOAD_COLUMN_FAMILY;
    private static byte[] rowLogColumnFamily = RowLogTableUtil.EXECUTIONSTATE_COLUMN_FAMILY;
    private static HTableInterface rowTable;
    private RowLogMessageConsumer consumer;
    private int consumerId = 0;
    private RowLogShard shard;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        control = createControl();
        rowTable = RowLogTableUtil.getRowTable(HBASE_PROXY.getConf());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
        rowLog = new RowLogImpl("RowLogTest", rowTable, payloadColumnFamily, rowLogColumnFamily, 60000L, null);
        consumer = control.createMock(RowLogMessageConsumer.class);
        consumer.getId();
        expectLastCall().andReturn(consumerId).anyTimes();
        
        consumer.getMaxTries();
        expectLastCall().andReturn(5).anyTimes();
        
        shard = control.createMock(RowLogShard.class);
        shard.getId();
        expectLastCall().andReturn("ShardId").anyTimes();
    }

    @After
    public void tearDown() throws Exception {
        control.reset();
    }
    
    @Test
    public void testRegisterConsumer() throws Exception {

        control.replay();
        rowLog.registerConsumer(consumer);
        Collection<RowLogMessageConsumer> consumers = rowLog.getConsumers();
        assertTrue(consumers.size() == 1);
        assertEquals(consumer, consumers.iterator().next());
        control.verify();
    }
    
    @Test
    public void testPutMessage() throws Exception {

        shard.putMessage(isA(RowLogMessage.class));
        
        control.replay();
        rowLog.registerConsumer(consumer);
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

        shard.putMessage(isA(RowLogMessage.class));
        expectLastCall().times(3);
        shard.removeMessage(isA(RowLogMessage.class), eq(consumerId));
        
        control.replay();
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row2");
        RowLogMessage message1 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message2 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message3 = rowLog.putMessage(rowKey, null, null, null);
        rowLog.messageDone(message2, consumerId, null);
        
        List<RowLogMessage> messages = rowLog.getMessages(rowKey, consumerId);
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
            rowLog.messageDone(message , 1, null);
            fail("Expected a MessageQueueException since no shards are registered");
        } catch (RowLogException expected) {
        }
        // Cleanup
        
        control.verify();
    }
    
    @Test
    public void testMessageConsumed() throws Exception {

        shard.putMessage(isA(RowLogMessage.class));
        shard.removeMessage(isA(RowLogMessage.class), eq(consumerId));
        
        control.replay();
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);

        byte[] lock = rowLog.lockMessage(message, consumerId);
        rowLog.messageDone(message, consumerId, lock);
        assertFalse(rowLog.isMessageLocked(message, consumerId));
        control.verify();
    }
    
    @Test
    public void testLockMessage() throws Exception {
        shard.putMessage(isA(RowLogMessage.class));
        
        control.replay();
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        
        assertNotNull(rowLog.lockMessage(message, consumerId));
        assertTrue(rowLog.isMessageLocked(message, consumerId));
        assertNull(rowLog.lockMessage(message, consumerId));
        control.verify();
    }
    
    @Test
    public void testUnlockMessage() throws Exception {
        shard.putMessage(isA(RowLogMessage.class));
        
        control.replay();
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
        
        byte[] lock = rowLog.lockMessage(message, consumerId);
        assertNotNull(lock);
        assertTrue(rowLog.unlockMessage(message, consumerId, lock));
        assertFalse(rowLog.isMessageLocked(message, consumerId));
        byte[] lock2 = rowLog.lockMessage(message, consumerId);
        assertNotNull(lock2);
        control.verify();
        //Cleanup 
        rowLog.unlockMessage(message, consumerId, lock2);
    }
    
    @Test
    public void testLockTimeout() throws Exception {
        rowLog = new RowLogImpl("RowLogTest", rowTable, payloadColumnFamily, rowLogColumnFamily, 1L, null);
        
        shard.putMessage(isA(RowLogMessage.class));
        
        control.replay();
        rowLog.registerConsumer(consumer);
        rowLog.registerShard(shard);
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
        
        byte[] lock = rowLog.lockMessage(message, consumerId );
        assertNotNull(lock);
        Thread.sleep(10L);
        assertFalse(rowLog.isMessageLocked(message, consumerId ));
        byte[] lock2 = rowLog.lockMessage(message, consumerId );
        assertNotNull(lock2);
        
        assertFalse(rowLog.unlockMessage(message, consumerId , lock));
        control.verify();
        //Cleanup
        rowLog.unlockMessage(message, consumerId , lock2);
    }
    
    @Test
    public void testLockingMultipleConsumers() throws Exception {
        RowLogMessageConsumer consumer1 = control.createMock(RowLogMessageConsumer.class);
        consumer1.getId();
        expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
        consumer1.getMaxTries();
        expectLastCall().andReturn(5).anyTimes();
        RowLogMessageConsumer consumer2 = control.createMock(RowLogMessageConsumer.class);
        consumer2.getId();
        expectLastCall().andReturn(Integer.valueOf(2)).anyTimes();
        consumer2.getMaxTries();
        expectLastCall().andReturn(5).anyTimes();

        shard.putMessage(isA(RowLogMessage.class));
        shard.removeMessage(isA(RowLogMessage.class), eq(2));
        
        control.replay();
        rowLog.registerConsumer(consumer1);
        rowLog.registerConsumer(consumer2);
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row2");
        RowLogMessage message = rowLog.putMessage(rowKey, null, null, null);
        
        byte[] lock = rowLog.lockMessage(message, 1);
        assertNotNull(lock);
        assertFalse(rowLog.isMessageLocked(message, 2));
        assertTrue(rowLog.unlockMessage(message, 1, lock));
        assertFalse(rowLog.isMessageLocked(message, 1));
        
        byte[] lock2 = rowLog.lockMessage(message, 2);
        assertNotNull(lock2);
        rowLog.messageDone(message, 2, lock2);
        assertFalse(rowLog.isMessageLocked(message, 2));
        
        control.verify();
        //Cleanup 
        rowLog.unlockMessage(message, 1, lock2);
    }
    
    @Test
    public void testgetMessages() throws Exception {
        RowLogMessageConsumer consumer1 = control.createMock(RowLogMessageConsumer.class);
        consumer1.getId();
        expectLastCall().andReturn(Integer.valueOf(1)).anyTimes();
        consumer1.getMaxTries();
        expectLastCall().andReturn(5).anyTimes();
        RowLogMessageConsumer consumer2 = control.createMock(RowLogMessageConsumer.class);
        consumer2.getId();
        expectLastCall().andReturn(Integer.valueOf(2)).anyTimes();
        consumer2.getMaxTries();
        expectLastCall().andReturn(5).anyTimes();
        
        shard.putMessage(isA(RowLogMessage.class));
        expectLastCall().times(2);
        shard.removeMessage(isA(RowLogMessage.class), eq(1));
        shard.removeMessage(isA(RowLogMessage.class), eq(2));
        
        control.replay();
        rowLog.registerConsumer(consumer1);
        rowLog.registerConsumer(consumer2);
        rowLog.registerShard(shard);
        byte[] rowKey = Bytes.toBytes("row3");
        RowLogMessage message1 = rowLog.putMessage(rowKey, null, null, null);
        RowLogMessage message2 = rowLog.putMessage(rowKey, null, null, null);

        byte[] lock = rowLog.lockMessage(message1, consumer1.getId());
        rowLog.messageDone(message1, consumer1.getId(), lock);
        lock = rowLog.lockMessage(message2, consumer2.getId());
        rowLog.messageDone(message2, consumer2.getId(), lock);
        
        List<RowLogMessage> messages = rowLog.getMessages(rowKey);
        assertEquals(2, messages.size());
        
        messages = rowLog.getMessages(rowKey, consumer1.getId());
        assertEquals(1, messages.size());
        assertEquals(message2, messages.get(0));
        
        messages = rowLog.getMessages(rowKey, consumer2.getId());
        assertEquals(1, messages.size());
        assertEquals(message1, messages.get(0));
        
        messages = rowLog.getMessages(rowKey, consumer1.getId(), consumer2.getId());
        assertEquals(2, messages.size());
        
        control.verify();
    }
}
