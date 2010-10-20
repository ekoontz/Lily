package org.lilycms.rowlog.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lilycms.rowlog.api.ListenersObserver;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogConfigurationManager;

public abstract class AbstractListenersSubscriptionHandler extends AbstractSubscriptionHandler implements ListenersObserver {
    protected ExecutorService executorService = Executors.newCachedThreadPool();
    protected RowLogConfigurationManager rowLogConfigurationManager;
    private Map<String, Future<?>> listeners = new ConcurrentHashMap<String, Future<?>>();
    protected volatile boolean stop = false;

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
        Future<?> future = executorService.submit(new Worker(listener));
        listeners.put(listener, future);
    }

    private void listenerUnregistered(String listenerId) {
        Future<?> future = listeners.get(listenerId);
        if (future != null)
            future.cancel(true);
        listeners.remove(listenerId);
    }
}
