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
import org.junit.Before;
import org.junit.Test;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogSubscription;
import org.lilycms.rowlog.impl.RemoteListenerHandler;

public class RowLogRemoteEndToEndTest extends AbstractRowLogEndToEndTest {

    private RemoteListenerHandler remoteListener;
    
    // Not in separate VM yet, but at least communication goes over channels.
    @Before
    public void setUp() throws Exception {
        System.out.println(">>RowLogRemoteEndToEndTest#"+name.getMethodName());
        validationListener = new ValidationMessageListener("VML1");
        subscriptionId = "Test";
        rowLogConfigurationManager.addSubscription(rowLog.getId(), subscriptionId,  RowLogSubscription.Type.Netty, 3, 1);
        waitForSubscription(subscriptionId);
        remoteListener = new RemoteListenerHandler(rowLog, subscriptionId, validationListener, rowLogConfigurationManager);
        remoteListener.start();
    }

    @After
    public void tearDown() throws Exception {
        remoteListener.stop();
        rowLogConfigurationManager.removeSubscription(rowLog.getId(), subscriptionId);
    }

    @Test(timeout=270000)
    public void testMultipleSubscriptions() throws Exception {
        ValidationMessageListener validationListener2 = new ValidationMessageListener("VML2");
        rowLogConfigurationManager.addSubscription(rowLog.getId(), "Test2", RowLogSubscription.Type.Netty, 3, 2);
        waitForSubscription(subscriptionId);
        RemoteListenerHandler remoteListener2 = new RemoteListenerHandler(rowLog, "Test2", validationListener2, rowLogConfigurationManager);
        remoteListener2.start();
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
        remoteListener2.stop();
        rowLogConfigurationManager.removeSubscription(rowLog.getId(), "Test2");
        validationListener.validate();
    }
}
