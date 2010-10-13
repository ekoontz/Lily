package org.lilycms.rowlog.impl.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;
import org.lilycms.rowlog.api.RowLogShard;

public class ValidationMessageListener implements RowLogMessageListener {

    private Map<RowLogMessage, Integer> expectedMessages = new HashMap<RowLogMessage, Integer>();
    private Map<RowLogMessage, Integer> earlyMessages = new HashMap<RowLogMessage, Integer>();
    public List<RowLogMessage> problematicMessages = new ArrayList<RowLogMessage>();
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
        if (earlyMessages.containsKey(message)) {
            int timesEarlyReceived = earlyMessages.get(message);
            count = count + timesEarlyReceived;
            int remainingTimes = times - timesEarlyReceived;
            if (remainingTimes < 0)
                throw new Exception("Recieved message <" + message + "> more than expected");
            earlyMessages.remove(message);
            if (remainingTimes > 0) {
                expectedMessages.put(message, remainingTimes);
            }
        } else {
            expectedMessages.put(message, times);
        }
    }

    public synchronized void expectMessages(int i) {
        numberOfMessagesToBeExpected = i;
    }

    public synchronized boolean processMessage(RowLogMessage message) {
        boolean result;
        if (!expectedMessages.containsKey(message)) {
            if (earlyMessages.containsKey(message)) {
                earlyMessages.put(message, earlyMessages.get(message) + 1);
            } else {
                earlyMessages.put(message, 1);
            }
            result = (!problematicMessages.contains(message));
        } else {
            count++;
            int timesRemaining = expectedMessages.get(message);
            if (timesRemaining == 1) {
                expectedMessages.remove(message);
                result = (!problematicMessages.contains(message));
            } else {
                expectedMessages.put(message, timesRemaining - 1);
                result = false;
            }
        }
        return result;
    }

    public void waitUntilMessagesConsumed(long timeout) throws Exception {
        long waitUntil = System.currentTimeMillis() + timeout;
        RowLogShard shard = rowLog.getShards().get(0);
        while ((!shard.next(subscriptionId).isEmpty() || !expectedMessages.isEmpty() || (count < numberOfMessagesToBeExpected))
                && System.currentTimeMillis() < waitUntil) {
            Thread.sleep(1000);
        }
    }

    public void validate() throws Exception {
        Assert.assertFalse(name + " Received less messages <" + count + "> than expected <"
                + numberOfMessagesToBeExpected + ">", (count < numberOfMessagesToBeExpected));
        Assert.assertFalse(name + " Received more messages <" + count + "> than expected <"
                + numberOfMessagesToBeExpected + ">", (count > numberOfMessagesToBeExpected));
        Assert.assertTrue(name + " EarlyMessages list is not empty <" + earlyMessages.keySet() + ">", earlyMessages
                .isEmpty());
        Assert.assertTrue(name + " Expected messages not processed within timeout", expectedMessages.isEmpty());
    }
}