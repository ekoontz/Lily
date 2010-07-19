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

import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;


public class RowLogProcessorTest {
    private IMocksControl control;
    private RowLog rowLog;
    private RowLogShard rowLogShard;
    private int consumerId;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        control = createControl();

        consumerId = 1;
        RowLogMessageConsumer consumer = control.createMock(RowLogMessageConsumer.class);
        consumer.getId();
        expectLastCall().andReturn(consumerId).anyTimes();

        rowLog = control.createMock(RowLog.class);
        rowLog.getId();
        expectLastCall().andReturn("TestRowLog").anyTimes();
        
        List<RowLogMessageConsumer> consumers = new ArrayList<RowLogMessageConsumer>();
        consumers.add(consumer);
        rowLog.getConsumers();
        expectLastCall().andReturn(consumers).anyTimes();
        
        rowLogShard = control.createMock(RowLogShard.class);
        rowLogShard.getId();
        expectLastCall().andReturn("TestShard").anyTimes();
        
        RowLogMessage message = control.createMock(RowLogMessage.class);
        List<RowLogMessage> messages = Arrays.asList(new RowLogMessage[] {message});
        rowLogShard.next(consumerId);
        expectLastCall().andReturn(messages).anyTimes();
        
        consumer.processMessage(message);
        expectLastCall().andReturn(Boolean.TRUE).anyTimes();
        rowLog.messageDone(eq(message), eq(consumerId), isA(byte[].class));
        expectLastCall().andReturn(Boolean.TRUE).anyTimes();
        
        rowLog.lockMessage(message, consumerId);
        byte[] someBytes = new byte[]{1,2,3};
        expectLastCall().andReturn(someBytes).anyTimes();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testProcessor() throws Exception {
        control.replay();
        RowLogProcessor processor = new RowLogProcessorImpl(rowLog, rowLogShard, null);
        assertFalse(processor.isRunning(consumerId));
        processor.start();
        assertTrue(processor.isRunning(consumerId));
        processor.stop();
        assertFalse(processor.isRunning(consumerId));
        control.verify();
    }
    
    @Test
    public void testProcessorMultipleStartStop() throws Exception {
        control.replay();
        RowLogProcessor processor = new RowLogProcessorImpl(rowLog, rowLogShard, null);
        assertFalse(processor.isRunning(consumerId));
        processor.start();
        assertTrue(processor.isRunning(consumerId));
        processor.stop();
        assertFalse(processor.isRunning(consumerId));
        processor.start();
        processor.start();
        assertTrue(processor.isRunning(consumerId));
        processor.stop();
        processor.stop();
        assertFalse(processor.isRunning(consumerId));
        control.verify();
    }
    
    @Test
    public void testProcessorStopWihtoutStart() throws Exception {
        control.replay();
        RowLogProcessor processor = new RowLogProcessorImpl(rowLog, rowLogShard, null);
        processor.stop();
        assertFalse(processor.isRunning(consumerId));
        processor.start();
        assertTrue(processor.isRunning(consumerId));
        processor.stop();
    }
}
