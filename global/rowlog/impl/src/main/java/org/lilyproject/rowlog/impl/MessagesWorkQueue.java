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
        if (messagesWorkingOn.contains(message)) {
            // Message is already being worked on
            return;
        }
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
    
    public int size() {
    	return messageQueue.size();
    }
}
