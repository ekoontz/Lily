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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.ProcessorNotifyObserver;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogProcessor;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.api.SubscriptionsObserver;
import org.lilyproject.util.Logs;

public class RowLogProcessorImpl implements RowLogProcessor, SubscriptionsObserver, ProcessorNotifyObserver {
    private volatile boolean stop = true;
    private final RowLog rowLog;
    private final RowLogShard shard;
    private final Map<String, SubscriptionThread> subscriptionThreads = Collections.synchronizedMap(new HashMap<String, SubscriptionThread>());
    private RowLogConfigurationManager rowLogConfigurationManager;
    private Log log = LogFactory.getLog(getClass());
    private long lastNotify = -1;
    private long notifyDelay = 100;
    
    public RowLogProcessorImpl(RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        this.rowLog = rowLog;
        this.rowLogConfigurationManager = rowLogConfigurationManager;
        this.shard = rowLog.getShards().get(0); // TODO: For now we only work with one shard
    }

    public RowLog getRowLog() {
        return rowLog;
    }

    @Override
    protected synchronized void finalize() throws Throwable {
        stop();
        super.finalize();
    }
    
    public synchronized void start() throws InterruptedException {
        if (stop) {
            stop = false;
            rowLogConfigurationManager.addSubscriptionsObserver(rowLog.getId(), this);
            rowLogConfigurationManager.addProcessorNotifyObserver(rowLog.getId(), shard.getId(), this);
        }
    }

    //  synchronized because we do not want to run this concurrently with the start/stop methods
    public synchronized void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
        synchronized (subscriptionThreads) {
            if (!stop) {
                List<String> newSubscriptionIds = new ArrayList<String>();
                for (RowLogSubscription newSubscription : newSubscriptions) {
                    newSubscriptionIds.add(newSubscription.getId());
                    if (!subscriptionThreads.containsKey(newSubscription.getId())) {
                        SubscriptionThread subscriptionThread = startSubscriptionThread(newSubscription);
                        subscriptionThreads.put(newSubscription.getId(), subscriptionThread);
                    }
                }
                Iterator<String> iterator = subscriptionThreads.keySet().iterator();
                while (iterator.hasNext()) {
                    String subscriptionId = iterator.next();
                    if (!newSubscriptionIds.contains(subscriptionId)) {
                        stopSubscriptionThread(subscriptionId);
                        iterator.remove();
                    }
                }
            }
        }
    }

    private SubscriptionThread startSubscriptionThread(RowLogSubscription subscription) {
        SubscriptionThread subscriptionThread = new SubscriptionThread(subscription);
        subscriptionThread.start();
        return subscriptionThread;
    }
    
    private void stopSubscriptionThread(String subscriptionId) {
        SubscriptionThread subscriptionThread = subscriptionThreads.get(subscriptionId);
        subscriptionThread.shutdown();
        try {
            Logs.logThreadJoin(subscriptionThread);
            subscriptionThread.join();
        } catch (InterruptedException e) {
        }
    }

    public synchronized void stop() {
        stop = true;
        rowLogConfigurationManager.removeSubscriptionsObserver(rowLog.getId(), this);
        rowLogConfigurationManager.removeProcessorNotifyObserver(rowLog.getId(), shard.getId());
        Collection<SubscriptionThread> threadsToStop;
        synchronized (subscriptionThreads) {
            threadsToStop = new ArrayList<SubscriptionThread>(subscriptionThreads.values());
            subscriptionThreads.clear();
        }
        for (SubscriptionThread thread : threadsToStop) {
            if (thread != null) {
                thread.shutdown();
            }
        }
        for (Thread thread : threadsToStop) {
            if (thread != null) {
                try {
                    if (thread.isAlive()) {
                        Logs.logThreadJoin(thread);
                        thread.join();
                    }
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public boolean isRunning(int consumerId) {
        return subscriptionThreads.get(consumerId) != null;
    }
    
    /**
     * Called when a message has been posted on the rowlog that needs to be processed by this RowLogProcessor. </p>
     * The notification will only be taken into account when a delay has passed since the previous notification.
     */
    synchronized public void notifyProcessor() {
    	long now = System.currentTimeMillis();
    	// Only consume the notification if the notifyDelay has expired to avoid too many wakeups
    	if ((lastNotify + notifyDelay) <= now) { 
    		lastNotify = now;
	    	Collection<SubscriptionThread> threadsToWakeup;
	        synchronized (subscriptionThreads) {
	            threadsToWakeup = new HashSet<SubscriptionThread>(subscriptionThreads.values());
	        }
	        for (SubscriptionThread subscriptionThread : threadsToWakeup) {
	            subscriptionThread.wakeup();
	        }
    	}
    }

    private class SubscriptionThread extends Thread {
        private long lastWakeup;
        private ProcessorMetrics metrics;
        private volatile boolean stopRequested = false; // do not rely only on Thread.interrupt since some libraries eat interruptions
        private MessagesWorkQueue messagesWorkQueue = new MessagesWorkQueue();
        private SubscriptionHandler subscriptionHandler;
        private String subscriptionId;
		private long wakeupTimeout;

        public SubscriptionThread(RowLogSubscription subscription) {
            super("Row log SubscriptionThread for " + subscription.getId());
            this.subscriptionId = subscription.getId();
            this.metrics = new ProcessorMetrics(rowLog.getId()+"_"+subscriptionId);
            switch (subscription.getType()) {
            case VM:
                subscriptionHandler = new LocalListenersSubscriptionHandler(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
                break;
                
            case Netty:
                subscriptionHandler = new RemoteListenersSubscriptionHandler(subscriptionId,  messagesWorkQueue, rowLog, rowLogConfigurationManager);

            default:
                break;
            }
        }
        
        public synchronized void wakeup() {
            metrics.wakeups.inc();
            lastWakeup = System.currentTimeMillis();
            this.notify();
        }
        
        @Override
        public synchronized void start() {
            stopRequested = false;
            subscriptionHandler.start();
            super.start();
        }
        
        public void shutdown() {
            stopRequested = true;
            subscriptionHandler.shutdown();
            interrupt();
        }
                
        public void run() {
            try {
                while (!isInterrupted() && !stopRequested) {
                    try {
                        metrics.scans.inc();
                        List<RowLogMessage> messages = shard.next(subscriptionId);

                        if (stopRequested) {
                            // Check if not stopped because HBase hides thread interruptions
                            return;
                        }

                        metrics.messagesPerScan.inc(messages != null ? messages.size() : 0);
                        if (messages != null && !messages.isEmpty()) {
                            for (RowLogMessage message : messages) {
                                if (stopRequested)
                                    return;

                                try {
                                    if (!rowLog.isMessageDone(message, subscriptionId) && !rowLog.isProblematic(message, subscriptionId)) {
                                        // The above calls to isMessageDone and isProblematic pass into HBase client code,
                                        // which, if interrupted, continue what it is doing and does not re-assert
                                        // the thread's interrupted status. By checking here that stopRequested is false,
                                        // we are sure that any interruption which comes after is is not ignored.
                                        // (The above about eating interruption status was true for HBase 0.89 beta
                                        // of October 2010).
                                        if (!stopRequested) {
                                            messagesWorkQueue.offer(message);
                                        }
                                    }
                                } catch (InterruptedException e) {
                                    return;
                                }
                            }
                        } else {
                            try {
                                wakeupTimeout = 5000;
                                long now = System.currentTimeMillis();
                                if (lastWakeup + wakeupTimeout < now) {
                                    synchronized (this) {
                                        wait(wakeupTimeout);
                                    }
                                }
                            } catch (InterruptedException e) {
                                // if we are interrupted, we stop working
                                return;
                            }
                        }
                    } catch (RowLogException e) {
                        // The message will be retried later
                        log.info("Error processing message for subscription " + subscriptionId + " (message will be retried later).", e);
                    } catch (Throwable t) {
                        if (Thread.currentThread().isInterrupted())
                            return;
                        log.error("Error in subscription thread for " + subscriptionId, t);
                    }
                }
            } finally {
                metrics.shutdown();
            }
        }
    }
}
