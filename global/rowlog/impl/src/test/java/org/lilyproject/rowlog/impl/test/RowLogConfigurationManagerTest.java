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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.rowlog.api.ListenersObserver;
import org.lilyproject.rowlog.api.ProcessorNotifyObserver;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.api.SubscriptionsObserver;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class RowLogConfigurationManagerTest {
    protected final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static ZooKeeperItf zooKeeper;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSubscription() throws Exception {
        String rowLogId = "testSubscriptionRowLogId";
        String subscriptionId1 = "testSubscriptionSubScriptionId1";
        String subscriptionId2 = "testSubscriptionSubScriptionId2";
        // Initialize
        RowLogConfigurationManagerImpl rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
        SubscriptionsCallBack callBack = new SubscriptionsCallBack();
        Assert.assertTrue(callBack.subscriptions.isEmpty());
        callBack.expect(Collections.<RowLogSubscription>emptyList());
        rowLogConfigurationManager.addSubscriptionsObserver(rowLogId, callBack);

        // After adding the observer we will receive an initial report of the subscriptions
        callBack.validate();

        // Add subscription
        RowLogSubscription expectedSubscriptionContext = new RowLogSubscription(rowLogId, subscriptionId1, Type.VM, 3, 1);
        callBack.expect(Arrays.asList(expectedSubscriptionContext));
        rowLogConfigurationManager.addSubscription(rowLogId, subscriptionId1, Type.VM, 3, 1);
        callBack.validate();

        RowLogSubscription expectedSubscriptionContext2 = new RowLogSubscription(rowLogId, subscriptionId2, Type.Netty, 5, 2);
        callBack.expect(Arrays.asList(expectedSubscriptionContext, expectedSubscriptionContext2));
        rowLogConfigurationManager.addSubscription(rowLogId, subscriptionId2, Type.Netty, 5, 2);
        callBack.validate();

        // Remove subscription
        callBack.expect(Arrays.asList(expectedSubscriptionContext2));
        rowLogConfigurationManager.removeSubscription(rowLogId, subscriptionId1);
        callBack.validate();
        
        callBack.expect(Collections.<RowLogSubscription>emptyList());
        rowLogConfigurationManager.removeSubscription(rowLogId, subscriptionId2);
        callBack.validate();

        rowLogConfigurationManager.shutdown();
    }
    
    private class SubscriptionsCallBack implements SubscriptionsObserver {
        public List<RowLogSubscription> subscriptions = new ArrayList<RowLogSubscription>();
        private List<RowLogSubscription> expectedSubscriptions;
        private Semaphore semaphore = new Semaphore(0);
        
        public void subscriptionsChanged(List<RowLogSubscription> subscriptions) {
            this.subscriptions = subscriptions;
            semaphore.release();
        }

        public void expect(List<RowLogSubscription> asList) {
            this.expectedSubscriptions = asList;
        }
        
        public void validate() throws Exception{
            semaphore.tryAcquire(10, TimeUnit.SECONDS);
            for (RowLogSubscription subscriptionContext : subscriptions) {
                Assert.assertTrue(expectedSubscriptions.contains(subscriptionContext));
            }
            for (RowLogSubscription subscriptionContext : expectedSubscriptions) {
                Assert.assertTrue(subscriptions.contains(subscriptionContext));
            }
        }
    }
    
    @Test
    public void testListener() throws Exception {
        String rowLogId = "testListenerRowLogId";
        String subscriptionId1 = "testListenerSubScriptionId1";
        // Initialize
        RowLogConfigurationManagerImpl rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zooKeeper);

        ListenersCallBack callBack = new ListenersCallBack();
        Assert.assertTrue(callBack.listeners.isEmpty());
        callBack.expect(Collections.<String>emptyList());
        rowLogConfigurationManager.addListenersObserver(rowLogId, subscriptionId1, callBack);

        // After adding the observer we will receive an initial report of the listeners
        callBack.validate();

        // Add subscription
        rowLogConfigurationManager.addSubscription(rowLogId, subscriptionId1, Type.VM, 3, 1);
        callBack.expect(Collections.EMPTY_LIST);
        callBack.validate();

        // Add listener
        callBack.expect(Arrays.asList(new String[]{"Listener1"}));
        rowLogConfigurationManager.addListener(rowLogId, subscriptionId1, "Listener1");
        callBack.validate();

        callBack.expect(Arrays.asList(new String[]{"Listener1", "Listener2"}));
        rowLogConfigurationManager.addListener(rowLogId, subscriptionId1, "Listener2");
        callBack.validate();

        // Remove subscription
        callBack.expect(Arrays.asList(new String[]{"Listener2"}));
        rowLogConfigurationManager.removeListener(rowLogId, subscriptionId1, "Listener1");
        callBack.validate();
        
        callBack.expect(Collections.EMPTY_LIST);
        rowLogConfigurationManager.removeListener(rowLogId, subscriptionId1, "Listener2");
        callBack.validate();

        rowLogConfigurationManager.shutdown();
    }
    
    private class ListenersCallBack implements ListenersObserver {
        public List<String> listeners = new ArrayList<String>();
        private List<String> expectedListeners;
        
        private Semaphore semaphore = new Semaphore(0);
        
        public void listenersChanged(List<String> listeners) {
            this.listeners = listeners;
            semaphore.release();
        }

        public void expect(List<String> expectedListeners) {
            semaphore.drainPermits();
            this.expectedListeners = expectedListeners;
        }
        
        private void validate() throws Exception {
            semaphore.tryAcquire(10, TimeUnit.SECONDS);
            for (String listener: listeners) {
                Assert.assertTrue(expectedListeners.contains(listener));
            }
            for (String listener : expectedListeners) {
                Assert.assertTrue(listeners.contains(listener));
            }
        }
    }

    @Test
    public void testProcessorNotify() throws Exception {
    	String rowLogId = "testProcessorNotifyRowLogId";
    	String shardId1 = "testProcessorNotifyShardId1";
    	String shardId2 = "testProcessorNotifyShardId2";
    	
        // Initialize
        RowLogConfigurationManagerImpl rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zooKeeper);

        ProcessorNotifyCallBack callBack1 = new ProcessorNotifyCallBack();
        ProcessorNotifyCallBack callBack2 = new ProcessorNotifyCallBack();

        // Add observers and expect an initial notify
        callBack1.expect(true);
        callBack2.expect(false);
        rowLogConfigurationManager.addProcessorNotifyObserver(rowLogId, shardId1, callBack1);
        callBack1.validate();
        callBack2.validate();

        callBack1.expect(false);
        callBack2.expect(true);
        rowLogConfigurationManager.addProcessorNotifyObserver(rowLogId, shardId2, callBack2);
        callBack1.validate();
        callBack2.validate();

        // Notify one processor
        callBack1.expect(true);
        callBack2.expect(false);
        rowLogConfigurationManager.notifyProcessor(rowLogId, shardId1);
        callBack1.validate();
        callBack2.validate();
    }
    
    private class ProcessorNotifyCallBack implements ProcessorNotifyObserver {
        
        private Semaphore semaphore = new Semaphore(0);
		private boolean expect = false;
        
        public void notifyProcessor() {
            semaphore.release();
        }

        public void expect(boolean expect) {
            this.expect  = expect;
			semaphore.drainPermits();
        }
        
        private void validate() throws Exception{
        	Assert.assertEquals(expect, semaphore.tryAcquire(10, TimeUnit.SECONDS));
        }
    }
    
}
