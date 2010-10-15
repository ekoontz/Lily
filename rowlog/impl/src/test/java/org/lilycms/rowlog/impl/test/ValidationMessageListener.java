package org.lilycms.rowlog.impl.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;
import org.lilycms.rowlog.api.RowLogShard;

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
        while ((!shard.next(subscriptionId).isEmpty() 
//                || !expectedMessages.isEmpty() 
                || (count < numberOfMessagesToBeExpected))
                && System.currentTimeMillis() < waitUntil) {
            Thread.sleep(1000);
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