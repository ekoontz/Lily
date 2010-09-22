package org.lilycms.rowlog.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.lilycms.rowlog.api.RowLogMessage;

public class MessagesWorkQueue {
    private BlockingQueue<RowLogMessage> messageQueue = new ArrayBlockingQueue<RowLogMessage>(1);
    private Set<RowLogMessage> messagesWorkingOn = Collections.synchronizedSet(new HashSet<RowLogMessage>());
    
    public boolean offer(RowLogMessage message) throws InterruptedException {
        if (!messageQueue.contains(message) && !messagesWorkingOn.contains(message)) {
            return messageQueue.offer(message, 1, TimeUnit.SECONDS);
        }
        return false;
    }
    
    public RowLogMessage poll() throws InterruptedException {
        if (messageQueue.isEmpty()) return null;
        synchronized (messageQueue) {
            RowLogMessage message = messageQueue.poll(1, TimeUnit.SECONDS);
            if (message == null) return null;
            messagesWorkingOn.add(message);
            return message;
        }
    }
    
    public void done(RowLogMessage message) {
        messagesWorkingOn.remove(message);
    }
}
