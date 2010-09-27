package org.lilycms.rowlog.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException;
import org.lilycms.rowlog.api.RowLog;

public abstract class AbstractListenersSubscriptionHandler extends AbstractSubscriptionHandler implements ListenersWatcherCallBack {
    protected ExecutorService executorService = Executors.newCachedThreadPool();
    private ExecutorService futuresExecutorService = Executors.newCachedThreadPool();
    protected RowLogConfigurationManagerImpl rowLogConfigurationManager;
    private Map<String, Future<?>> listeners = new ConcurrentHashMap<String, Future<?>>();
    protected volatile boolean stop = false;

    public AbstractListenersSubscriptionHandler(int subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog, RowLogConfigurationManagerImpl rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog);
        this.rowLogConfigurationManager = rowLogConfigurationManager;
    }

    public void start() {
        try {
            listenersChanged(rowLogConfigurationManager.getAndMonitorListeners(this, rowLogId, subscriptionId));
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void interrupt() {
        stop = true;
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
        // TODO resubmit ?
//        futuresExecutorService.submit(new Resubmitter(listener, future));
    }

    private void listenerUnregistered(String listenerId) {
        Future<?> future = listeners.get(listenerId);
        if (future != null)
            future.cancel(true);
        listeners.remove(listenerId);
    }
    
    private class Resubmitter implements Callable<Object> {
        private final Future<?> future;
        private final String listenerId;

        public Resubmitter(String listenerId, Future<?> future) {
            this.listenerId = listenerId;
            this.future = future;
        }
        
        public Object call() {
            try {
                future.get();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // Stop 
                return null;
            } 
            if (!stop)
                submitWorker(listenerId);
            return null;
        }
    }
}
