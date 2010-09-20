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
import org.lilycms.rowlog.api.SubscriptionContext;
import org.lilycms.rowlog.impl.RowLogConfigurationManager;

public class RowLogEmbededEndToEndTest extends AbstractRowLogEndToEndTest {
    
    @Before
    public void setUp() throws Exception {
        rowLogConfigurationManager = new RowLogConfigurationManager(zkConnectString);
        consumer = new TestMessageConsumer(0);
        rowLog.registerConsumer(consumer);
        rowLogConfigurationManager.addSubscription(rowLog.getId(), consumer.getId(),  SubscriptionContext.Type.Embeded, 5);
    }

    @After
    public void tearDown() throws Exception {
        assertHostAndPortRemovedFromZK();
        consumer.validate();
        rowLogConfigurationManager.removeSubscription(rowLog.getId(), consumer.getId());
        rowLog.unRegisterConsumer(consumer);
        rowLogConfigurationManager.stop();
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        TestMessageConsumer consumer2 = new TestMessageConsumer(1);
        rowLog.registerConsumer(consumer2);
        rowLogConfigurationManager.addSubscription(rowLog.getId(), consumer2.getId(), SubscriptionContext.Type.Embeded, 5);
        consumer.expectMessages(10);
        consumer2.expectMessages(10);
        RowLogMessage message;
        for (long seqnr = 0L; seqnr < 2; seqnr++) {
            for (int rownr = 20; rownr < 25; rownr++) {
                byte[] data = Bytes.toBytes(rownr);
                data = Bytes.add(data, Bytes.toBytes(seqnr));
                message = rowLog.putMessage(Bytes.toBytes("row" + rownr), data, null, null);
                consumer.expectMessage(message);
                consumer2.expectMessage(message);
            }
        }
        processor.start();
        consumer.waitUntilMessagesConsumed(120000);
        consumer2.waitUntilMessagesConsumed(120000);
        processor.stop();
        consumer2.validate();
        rowLog.unRegisterConsumer(consumer2);
        rowLogConfigurationManager.removeSubscription(rowLog.getId(), consumer2.getId());
    }

    
}
