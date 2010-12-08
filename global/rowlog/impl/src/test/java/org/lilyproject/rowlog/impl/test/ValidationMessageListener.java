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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.rowlog.api.RowLogShard;

public class ValidationMessageListener implements RowLogMessageListener {

    private Map<RowLogMessage, Integer> expectedMessages = new HashMap<RowLogMessage, Integer>();
    private Map<RowLogMessage, Integer> processedMessages = new HashMap<RowLogMessage, Integer>();
    public List<RowLogMessage> messagesToBehaveAsProblematic = new ArrayList<RowLogMessage>();
    private int count = 0;
    private int numberOfMessagesToBeExpected = 0;
    private final String name;
    private final RowLog rowLog;
    private final String subscriptionId;

    public ValidationMessageListener(String name, String subscriptionId, RowLog rowLog) {
        this.name = name;
        this.subscriptionId = subscriptionId;
        this.rowLog = rowLog;
    }

    public synchronized void expectMessage(RowLogMessage message) throws Exception {
        expectMessage(message, 1);
    }

    public synchronized void expectMessage(RowLogMessage message, int times) throws Exception {
        expectedMessages.put(message, times);
    }

    public synchronized void expectMessages(int i) {
        numberOfMessagesToBeExpected = i;
    }

    public synchronized boolean processMessage(RowLogMessage message) {
        count++;
        Integer times = processedMessages.get(message);
        if (times == null) {
            processedMessages.put(message, 1);
        } else {
            processedMessages.put(message, times+1);
        }
        if (messagesToBehaveAsProblematic.contains(message)) {
            return false;
        }
        return true;
    }

    public void waitUntilMessagesConsumed(long timeout) throws Exception {
        long waitUntil = System.currentTimeMillis() + timeout;
        RowLogShard shard = rowLog.getShards().get(0);
        List<RowLogMessage> messages = shard.next(subscriptionId);
        while(true) {
            if (System.currentTimeMillis() >= waitUntil) {
                System.out.println("ValidationMessageListener#waitUntilMessagesConsumed: Timeout expired");
                break;
            }
            if (messages.isEmpty() && count >= numberOfMessagesToBeExpected) {
                System.out.println("ValidationMessageListener#waitUntilMessagesConsumed: No more messages and count reached");
                break;
            }
            Thread.sleep(1000);
            messages = shard.next(subscriptionId);
        }
    }

    public void validate() throws Exception {
        boolean success = true;
        StringBuilder validationMessage = new StringBuilder();
        
        if (!(numberOfMessagesToBeExpected == count)) {
            success = false;
            validationMessage.append("\n"+name+ " did not process the same amount of messages <"+count+"> as expected <"+numberOfMessagesToBeExpected+">");
        }
        for (Entry<RowLogMessage, Integer> entry: processedMessages.entrySet()) {
            if (!expectedMessages.containsKey(entry.getKey())) {
                success = false;
                validationMessage.append("\n"+name+ " did not expect to receive message <"+entry.getKey()+">");
            } else {
                Integer expectedTimes = expectedMessages.get(entry.getKey());
                if (!expectedTimes.equals(entry.getValue())) {
                    success = false;
                    validationMessage.append("\n"+name+ " did not receive message <"+entry.getKey()+"> the number of expected times: <"+entry.getValue()+"> instead of <" +expectedTimes+ ">");
                }
            }
        }
        for (RowLogMessage expectedMessage : expectedMessages.keySet()) {
            if (!processedMessages.containsKey(expectedMessage)) {
                success = false;
                validationMessage.append("\n"+name+ " did not receive expected message <" +expectedMessage+ ">");
            }
        }
        Assert.assertTrue(validationMessage.toString(), success);
    }
}