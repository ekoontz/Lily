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


import static org.easymock.classextension.EasyMock.createControl;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;
import org.lilycms.rowlog.impl.RowLogMessageImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

public class RowLogShardTest {

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static RowLogShardImpl shard;
    private static IMocksControl control;
    private static RowLog rowLog;
    private static int batchSize = 5;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        control = createControl();
        rowLog = control.createMock(RowLog.class);
        shard = new RowLogShardImpl("TestShard", HBASE_PROXY.getConf(), rowLog, batchSize);
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
        control.reset();
    }
    
    @Test
    public void testSingleMessage() throws Exception {
        int consumerId = 1;

        RowLogMessageListener consumer = control.createMock(RowLogMessageListener.class);
        consumer.getId();
        expectLastCall().andReturn(new Integer(consumerId));
        
        rowLog.getConsumers();
        expectLastCall().andReturn(Arrays.asList(new RowLogMessageListener[]{consumer}));
        
        control.replay();
        byte[] messageId1 = Bytes.toBytes("messageId1");
        RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 0L, null, rowLog);
        shard.putMessage(message1);
        
        List<RowLogMessage> messages = shard.next(consumerId);
        assertEquals(1, messages.size());
        assertEquals(message1, messages.get(0));
        
        shard.removeMessage(message1, consumerId);
        assertTrue(shard.next(consumerId).isEmpty());
        control.verify();
    }
    
    @Test
    public void testMultipleMessages() throws Exception {
        int consumerId = 1;
        
        RowLogMessageListener consumer = control.createMock(RowLogMessageListener.class);
        consumer.getId();
        expectLastCall().andReturn(new Integer(consumerId)).anyTimes();
        
        rowLog.getConsumers();
        expectLastCall().andReturn(Arrays.asList(new RowLogMessageListener[]{consumer})).anyTimes();
        
        control.replay();
        byte[] messageId1 = Bytes.toBytes("messageId1");
        RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 0L, null, rowLog);
        byte[] messageId2 = Bytes.toBytes("messageId2");
        RowLogMessageImpl message2 = new RowLogMessageImpl(messageId2, Bytes.toBytes("row2"), 0L, null, rowLog);

        shard.putMessage(message1);
        shard.putMessage(message2);

        List<RowLogMessage> messages = shard.next(consumerId);
        assertEquals(2, messages.size());
        assertEquals(message1, messages.get(0));

        shard.removeMessage(message1, consumerId);
        assertEquals(message2, messages.get(1));
        
        shard.removeMessage(message2, consumerId);
        assertTrue(shard.next(consumerId).isEmpty());
        control.verify();
    }
    
    @Test
    public void testBatchSize() throws Exception {
        int consumerId = 1;
        
        RowLogMessageListener consumer = control.createMock(RowLogMessageListener.class);
        consumer.getId();
        expectLastCall().andReturn(new Integer(consumerId)).anyTimes();
        
        rowLog.getConsumers();
        expectLastCall().andReturn(Arrays.asList(new RowLogMessageListener[]{consumer})).anyTimes();
        
        RowLogMessage[] expectedMessages = new RowLogMessage[7];
        control.replay();
        for (int i = 0; i < 7; i++) {
            RowLogMessageImpl message = new RowLogMessageImpl(Bytes.toBytes("messageId" + i), Bytes.toBytes("row1"), 0L, null, rowLog);
            expectedMessages[i] = message;
            shard.putMessage(message);
        }

        List<RowLogMessage> messages = shard.next(consumerId);
        assertEquals(batchSize , messages.size());

        int i = 0;
        for (RowLogMessage message : messages) {
            assertEquals(expectedMessages[i++], message);
            shard.removeMessage(message, consumerId);
        }
        messages = shard.next(consumerId);
        assertEquals(2, messages.size());
        for (RowLogMessage message : messages) {
            assertEquals(expectedMessages[i++], message);
            shard.removeMessage(message, consumerId);
        }
        
        assertTrue(shard.next(consumerId).isEmpty());
        control.verify();
    }
    
    
    @Test
    public void testMultipleConsumers() throws Exception {
        int consumerId1 = 1;
        int consumerId2 = 2;

        RowLogMessageListener consumer1 = control.createMock(RowLogMessageListener.class);
        consumer1.getId();
        expectLastCall().andReturn(new Integer(consumerId1)).anyTimes();

        RowLogMessageListener consumer2 = control.createMock(RowLogMessageListener.class);
        consumer2.getId();
        expectLastCall().andReturn(new Integer(consumerId2)).anyTimes();
        
        rowLog.getConsumers();
        expectLastCall().andReturn(Arrays.asList(new RowLogMessageListener[]{consumer1, consumer2})).anyTimes();
        
        control.replay();
        byte[] messageId1 = Bytes.toBytes("messageId1");
        RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 1L, null, rowLog);
        byte[] messageId2 = Bytes.toBytes("messageId2");
        RowLogMessageImpl message2 = new RowLogMessageImpl(messageId2, Bytes.toBytes("row2"), 1L, null, rowLog);
        
        shard.putMessage(message1);
        shard.putMessage(message2);
        List<RowLogMessage> messages = shard.next(consumerId1);
        assertEquals(2, messages.size());
        assertEquals(message1, messages.get(0));
        shard.removeMessage(message1, consumerId1);
        assertEquals(message2, messages.get(1));
        shard.removeMessage(message2, consumerId1);
        
        messages = shard.next(consumerId2);
        assertEquals(2, messages.size());
        assertEquals(message1, messages.get(0));
        shard.removeMessage(message1, consumerId2);
        assertEquals(message2, messages.get(1));
        shard.removeMessage(message2, consumerId2);
        
        assertTrue(shard.next(consumerId1).isEmpty());
        assertTrue(shard.next(consumerId2).isEmpty());
        control.verify();
    }
    
    @Test
    public void testMessageDoesNotExistForConsumer() throws Exception {
        int consumerId1 = 1;
        int consumerId2 = 2;
        
        RowLogMessageListener consumer = control.createMock(RowLogMessageListener.class);
        consumer.getId();
        expectLastCall().andReturn(new Integer(consumerId1)).anyTimes();
        
        rowLog.getConsumers();
        expectLastCall().andReturn(Arrays.asList(new RowLogMessageListener[]{consumer})).anyTimes();
        
        control.replay();
        byte[] messageId1 = Bytes.toBytes("messageId1");
        RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 1L, null, rowLog);

        shard.putMessage(message1);
        assertTrue(shard.next(consumerId2).isEmpty());

        shard.removeMessage(message1, consumerId2);
        List<RowLogMessage> messages = shard.next(consumerId1);
        assertEquals(message1, messages.get(0));
        // Cleanup
        shard.removeMessage(message1, consumerId1);
        assertTrue(shard.next(consumerId1).isEmpty());
        control.verify();
    }

    @Test
    public void testProblematicMessage() throws Exception {
        int consumerId = 1;

        RowLogMessageListener consumer = control.createMock(RowLogMessageListener.class);
        consumer.getId();
        expectLastCall().andReturn(new Integer(consumerId));
        
        rowLog.getConsumers();
        expectLastCall().andReturn(Arrays.asList(new RowLogMessageListener[]{consumer}));
        
        control.replay();
        byte[] messageId1 = Bytes.toBytes("messageId1");
        RowLogMessageImpl message1 = new RowLogMessageImpl(messageId1, Bytes.toBytes("row1"), 0L, null, rowLog);
        shard.putMessage(message1);
        
        shard.markProblematic(message1, consumerId);
        
        List<RowLogMessage> messages = shard.getProblematic(consumerId);
        assertEquals(1, messages.size());
        assertEquals(message1, messages.get(0));
        
        assertTrue(shard.next(consumerId).isEmpty());
        
        shard.removeMessage(message1, consumerId);
        assertTrue(shard.getProblematic(consumerId).isEmpty());
        assertTrue(shard.next(consumerId).isEmpty());
        control.verify();
    }

}
