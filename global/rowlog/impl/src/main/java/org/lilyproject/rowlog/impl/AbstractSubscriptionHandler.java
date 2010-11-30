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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.Logs;

public abstract class AbstractSubscriptionHandler implements SubscriptionHandler {
    protected final RowLog rowLog;
    protected final String rowLogId;
    protected final String subscriptionId;
    protected final MessagesWorkQueue messagesWorkQueue;
    private Log log = LogFactory.getLog(getClass());
	private SubscriptionHandlerMetrics metrics;
    
    public AbstractSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog) {
        this.rowLog = rowLog;
        this.rowLogId = rowLog.getId();
        this.subscriptionId = subscriptionId;
        this.messagesWorkQueue = messagesWorkQueue;
        this.metrics = new SubscriptionHandlerMetrics(rowLog.getId()+"_"+subscriptionId);
    }
    
    protected abstract boolean processMessage(String context, RowLogMessage message) throws InterruptedException;
    
    protected class Worker implements Runnable {
        private final String subscriptionId;
        private final String listener;
        private Thread thread;
        private volatile boolean stop; // do not rely only on Thread.interrupt since some libraries eat interruptions

        public Worker(String subscriptionId, String listener) {
            this.subscriptionId = subscriptionId;
            this.listener = listener;
        }

        public void start() {
            thread = new Thread(this, "Handler: subscription " + subscriptionId + ", listener " + listener);
            thread.start();
        }

        public void stop() throws InterruptedException {
            stop = true;
            thread.interrupt();
            Logs.logThreadJoin(thread);
            thread.join();
        }

        public void run() {
            while(!stop && !Thread.interrupted()) {
                RowLogMessage message;
                try {
                	metrics.queueSize.set(messagesWorkQueue.size());
                    message = messagesWorkQueue.take();
                    if (message != null) {
                        try {
                            byte[] lock = rowLog.lockMessage(message, subscriptionId);
                            if (lock != null) {
                                if (!rowLog.isMessageAvailable(message, subscriptionId) || rowLog.isProblematic(message, subscriptionId)) {
                                    rowLog.unlockMessage(message, subscriptionId, false, lock);
                                } else {
                                    if (processMessage(listener, message)) {
                                    	metrics.successRate.inc();
                                        rowLog.messageDone(message, subscriptionId, lock);
                                    } else {
                                    	metrics.failureRate.inc();
                                        rowLog.unlockMessage(message, subscriptionId, true, lock);
                                    }
                                }
                            }
                        } catch (InterruptedException e) {
                            break;                            
                        } catch (Throwable e) {
                            log.warn(String.format("RowLogException occurred while processing message %1$s by subscription %2$s of rowLog %3$s", message, subscriptionId, rowLogId), e);
                        } finally {
                            messagesWorkQueue.done(message);
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
