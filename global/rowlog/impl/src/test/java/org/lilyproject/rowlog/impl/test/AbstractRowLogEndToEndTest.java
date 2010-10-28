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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogProcessor;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogProcessorImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

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
        Closer.close(rowLogConfigurationManager);
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }
    
    @Test(timeout=150000)
    public void testSingleMessage() throws Exception {
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        validationListener.expectMessage(message);
        validationListener.expectMessages(1);
        processor.start();
        validationListener.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener.validate();
    }

    @Test(timeout=150000)
    public void testSingleMessageProcessorStartsFirst() throws Exception {
        validationListener.expectMessages(1);
        processor.start();
        System.out.println(">>RowLogEndToEndTest#"+name.getMethodName()+": processor started");
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row2"), null, null, null);
        validationListener.expectMessage(message);
        System.out.println(">>RowLogEndToEndTest#"+name.getMethodName()+": waiting for message to be processed");
        validationListener.waitUntilMessagesConsumed(120000);
        System.out.println(">>RowLogEndToEndTest#"+name.getMethodName()+": message processed");
        processor.stop();
        System.out.println(">>RowLogEndToEndTest#"+name.getMethodName()+": processor stopped");
        validationListener.validate();
    }

    @Test(timeout=150000)
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

    @Test(timeout=150000)
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

    @Test(timeout=150000)
    public void testProblematicMessage() throws Exception {
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row1"), null, null, null);
        validationListener.messagesToBehaveAsProblematic.add(message);
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
