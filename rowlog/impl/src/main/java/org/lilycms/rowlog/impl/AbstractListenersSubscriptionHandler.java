package org.lilycms.rowlog.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.rowlog.api.ListenersObserver;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogConfigurationManager;

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
