package org.lilyproject.rowlog.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.lilyproject.rowlog.api.RowLogMessage;

public class MessagesWorkQueue {
    private BlockingQueue<RowLogMessage> messageQueue = new ArrayBlockingQueue<RowLogMessage>(100);
    private Set<RowLogMessage> messagesWorkingOn = Collections.synchronizedSet(new HashSet<RowLogMessage>());
    
    public void offer(RowLogMessage message) throws InterruptedException {
        if (!messageQueue.contains(message)) {
            messageQueue.put(message);
        }
    }
    
    public RowLogMessage take() throws InterruptedException {
        RowLogMessage message = messageQueue.take();
        synchronized (messagesWorkingOn) {
            if (messageQueue.contains(message)) {
             // Too late, the message has been put on the queue already again after the take() call.                
                return null; 
            }
            messagesWorkingOn.add(message);
            return message;
        }
    }
    
    public void done(RowLogMessage message) {
        synchronized (messagesWorkingOn) {
            messagesWorkingOn.remove(message);
        }
    }
}
