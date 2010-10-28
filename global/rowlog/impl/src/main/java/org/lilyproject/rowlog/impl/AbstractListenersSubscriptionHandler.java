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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.ListenersObserver;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;

public abstract class AbstractListenersSubscriptionHandler extends AbstractSubscriptionHandler implements ListenersObserver {
    protected RowLogConfigurationManager rowLogConfigurationManager;
    private Map<String, Worker> listeners = new ConcurrentHashMap<String, Worker>();
    protected volatile boolean stop = false;
    private Log log = LogFactory.getLog(getClass());

    public AbstractListenersSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue,
            RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog);
        this.rowLogConfigurationManager = rowLogConfigurationManager;
    }

    public void start() {
        rowLogConfigurationManager.addListenersObserver(rowLogId, subscriptionId, this);
    }

    public void shutdown() {
        stop = true;
        rowLogConfigurationManager.removeListenersObserver(rowLogId, subscriptionId, this);
        for (String listenerId : listeners.keySet())
            listenerUnregistered(listenerId);
    }

    public void listenersChanged(List<String> newListeners) {
        if (!stop) {
            for (String newListener : newListeners) {
                if (!listeners.containsKey(newListener))
                    listenerRegistered(newListener);
            }
            for (String listenerId : listeners.keySet()) {
                if (!newListeners.contains(listenerId))
                    listenerUnregistered(listenerId);
            }
        }
    }

    private void listenerRegistered(String listener) {
        submitWorker(listener);
    }

    protected void submitWorker(String listener) {
        Worker worker = new Worker(subscriptionId, listener);
        worker.start();
        listeners.put(listener, worker);
    }

    private void listenerUnregistered(String listenerId) {
        Worker worker = listeners.get(listenerId);
        if (worker != null) {
            try {
                worker.stop();
            } catch (InterruptedException e) {
                log.info("Interrupted while stopping subscription handler worker.", e);
            }
            listeners.remove(listenerId);
        }
    }
}
