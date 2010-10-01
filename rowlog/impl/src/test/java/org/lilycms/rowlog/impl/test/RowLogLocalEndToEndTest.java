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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.SubscriptionContext;
import org.lilycms.rowlog.impl.RowLogMessageListenerMapping;

public class RowLogLocalEndToEndTest {
    @Test
    public void foo() {
        
    }
}/*
extends AbstractRowLogEndToEndTest {

    private ValidationMessageListener validationListener2;

    @Before
    public void setUp() throws Exception {
        try {
            validationListener = new ValidationMessageListener();
            RowLogMessageListenerMapping.INSTANCE.put(subscriptionId , validationListener);
            rowLogConfigurationManager.addSubscription(rowLog.getId(), subscriptionId,  SubscriptionContext.Type.VM, 3, 1);
            rowLogConfigurationManager.addListener(rowLog.getId(), subscriptionId, "listener1");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            rowLogConfigurationManager.removeListener(rowLog.getId(), subscriptionId, "listener1");
            rowLogConfigurationManager.removeSubscription(rowLog.getId(), subscriptionId);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testMultipleSubscriptions() throws Exception {
        validationListener2 = new ValidationMessageListener();
        String subscriptionId2 = "Subscription2";
        RowLogMessageListenerMapping.INSTANCE.put(subscriptionId2, validationListener2);
        rowLogConfigurationManager.addSubscription(rowLog.getId(), subscriptionId2, SubscriptionContext.Type.VM, 3, 2);
        rowLogConfigurationManager.addListener(rowLog.getId(), subscriptionId2, "Listener2");
        validationListener.expectMessages(10);
        validationListener2.expectMessages(10);
        RowLogMessage message;
        for (long seqnr = 0L; seqnr < 2; seqnr++) {
            for (int rownr = 20; rownr < 25; rownr++) {
                byte[] data = Bytes.toBytes(rownr);
                data = Bytes.add(data, Bytes.toBytes(seqnr));
                message = rowLog.putMessage(Bytes.toBytes("row" + rownr), data, null, null);
                validationListener.expectMessage(message);
                validationListener2.expectMessage(message);
            }
        }
        processor.start();
        validationListener.waitUntilMessagesConsumed(120000);
        validationListener2.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener2.validate();
        rowLogConfigurationManager.removeListener(rowLog.getId(), subscriptionId2, "Listener2");
        rowLogConfigurationManager.removeSubscription(rowLog.getId(), subscriptionId2);
        validationListener.validate();
    }
    
    @Test
    public void testMultipleSubscriptionsOrder() throws Exception {
        validationListener2 = new ValidationMessageListener();
        String subscriptionId2 = "Subscription2";
        RowLogMessageListenerMapping.INSTANCE.put(subscriptionId2, validationListener2);
        rowLogConfigurationManager.addSubscription(rowLog.getId(), subscriptionId2, SubscriptionContext.Type.VM, 3, 0);
        rowLogConfigurationManager.addListener(rowLog.getId(), subscriptionId2, "Listener2");
        int rownr = 222;
        byte[] data = Bytes.toBytes(222);
        data = Bytes.add(data, Bytes.toBytes(0));
        RowLogMessage message = rowLog.putMessage(Bytes.toBytes("row" + rownr), data, null, null);
        validationListener.expectMessages(1);
        validationListener.expectMessage(message);
        validationListener2.expectMessage(message, 3);
        validationListener2.expectMessages(3);
        validationListener2.problematicMessages.add(message);

        processor.start();
        validationListener2.waitUntilMessagesConsumed(120000);
        processor.stop();
        validationListener2.validate();
     // Assert the message was not processed by subscription1 (last in order) and was marked problematic 
     // since subscription2 (first in order) became problematic
        Assert.assertTrue(rowLog.isProblematic(message, subscriptionId)); 
        rowLogConfigurationManager.removeListener(rowLog.getId(), subscriptionId2, "Listener2");
        rowLogConfigurationManager.removeSubscription(rowLog.getId(), subscriptionId2);
    }
}
*/
