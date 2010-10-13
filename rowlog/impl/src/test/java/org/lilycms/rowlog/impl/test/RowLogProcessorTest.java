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

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;


public class RowLogProcessorTest {
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
    
    @Test
    public void testProcessorPublishesHost() throws Exception {
        Assert.assertTrue("Expected processorHost to be null", rowLogConfigurationManager.getProcessorHost(rowLog.getId(), shard.getId()) == null);
        processor.start();
        assertNotNull("Expected processorHost to exist", rowLogConfigurationManager.getProcessorHost(rowLog.getId(), shard.getId()));
        processor.stop();
        Assert.assertTrue("Expected processorHost to be null", rowLogConfigurationManager.getProcessorHost(rowLog.getId(), shard.getId()) == null);
    }
}
