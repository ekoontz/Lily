package org.lilycms.rowlog.impl;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.lilycms.rowlog.api.*;
import org.lilycms.rowlog.api.SubscriptionContext.Type;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import javax.annotation.PreDestroy;

public class RowLogConfigurationManagerImpl implements RowLogConfigurationManager {
    private String lilyPath = "/lily";
    private String rowLogPath = lilyPath + "/rowlog";
    
    private ZooKeeperItf zooKeeper;

    private ObserverSupport observerSupport = new ObserverSupport();

    private Log log = LogFactory.getLog(getClass());
    
    public RowLogConfigurationManagerImpl(ZooKeeperItf zooKeeper) throws RowLogException {
        this.zooKeeper = zooKeeper;
        this.observerSupport = new ObserverSupport();
        observerSupport.start();
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        observerSupport.shutdown();
    }
    
    // Subscriptions

    public void addSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer) {
        observerSupport.addSubscriptionsObserver(rowLogId, observer);
    }

    public void removeSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer) {
        observerSupport.removeSubscriptionsObserver(rowLogId, observer);
    }

    public synchronized void addSubscription(String rowLogId, String subscriptionId, SubscriptionContext.Type type, int maxTries, int orderNr) throws KeeperException, InterruptedException {
        String path = subscriptionPath(rowLogId, subscriptionId);
        byte[] data = Bytes.toBytes(maxTries);
        data = Bytes.add(data, Bytes.toBytes(orderNr));
        data = Bytes.add(data, Bytes.toBytes(type.name()));
        if (zooKeeper.exists(path, false) == null) { // TODO currently not possible to update a subscription or add it twice
            ZkUtil.createPath(zooKeeper, path, data, CreateMode.PERSISTENT);
        }
    }
    
    public synchronized void removeSubscription(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException {
        String path = subscriptionPath(rowLogId, subscriptionId);
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException.NoNodeException ignore) {
        } catch (KeeperException.NotEmptyException e) {
            // TODO Think what should happen here
            // Remove listeners first?
        }
    }
    
    // Listeners

    public void addListenersObserver(String rowLogId, String subscriptionId, ListenersObserver observer) {
        observerSupport.addListenersObserver(new ListenerKey(rowLogId, subscriptionId), observer);
    }

    public void removeListenersObserver(String rowLogId, String subscriptionId, ListenersObserver observer) {
        observerSupport.removeListenersObserver(new ListenerKey(rowLogId, subscriptionId), observer);
    }

    public void addListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException {
        String path = listenerPath(rowLogId, subscriptionId, listenerId);
        try {
            if (zooKeeper.exists(path, false) == null) {
                ZkUtil.createPath(zooKeeper, path, null, CreateMode.EPHEMERAL);
            }
        } catch (Exception e) {
            throw new RowLogException("Failed to add listener to rowlog configuration", e);
        }
    }
    
    public void removeListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException {
        String path = listenerPath(rowLogId, subscriptionId, listenerId);
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException.NoNodeException ignore) {
        } catch (Exception e) {
            throw new RowLogException("Failed to remove listener from rowlog configuration", e);
        } 
    }
    
    // Processor Host
    public void publishProcessorHost(String hostName, int port, String rowLogId, String shardId) {
        String path = processorPath(rowLogId, shardId);
        try {
            if (zooKeeper.exists(path, false) == null) {
                    ZkUtil.createPath(zooKeeper, path);
            }
            zooKeeper.setData(path, Bytes.toBytes(hostName + ":" + port), -1);
        } catch (Exception e) {
            //TODO log? throw?
        }
    }
    
    public void unPublishProcessorHost(String rowLogId, String shardId) {
        try {
            zooKeeper.delete(processorPath(rowLogId, shardId), -1);
        } catch (Exception e) {
            //TODO log? throw?
        }
    }

    public String getProcessorHost(String rowLogId, String shardId) {
        try {
            return Bytes.toString(zooKeeper.getData(processorPath(rowLogId, shardId), false, new Stat()));
        } catch (Exception e) {
            return null;
        }
    }
    
    // Paths
    private String subscriptionPath(String rowLogId, String subscriptionId) {
        return subscriptionsPath(rowLogId) + "/" + subscriptionId;
    }
    
    private String subscriptionsPath(String rowLogId) {
        return rowLogPath + "/" + rowLogId + "/subscriptions";
    }
    
    private String shardPath(String rowLogId, String shardId) {
        return rowLogPath + "/" + rowLogId + "/shards" + "/" + shardId;
    }
    
    private String processorPath(String rowLogId, String shardId) {
        return shardPath(rowLogId, shardId) + "/" + "processorHost";
    }
    
    private String listenerPath(String rowLogId, String subscriptionId, String listenerId) {
        return subscriptionPath(rowLogId, subscriptionId) + "/" + listenerId;
    }
    

    private class ObserverSupport implements Runnable {
        /** key = row log id. */
        private Map<String, SubscriptionsObservers> subscriptionsObservers = Collections.synchronizedMap(new HashMap<String, SubscriptionsObservers>());
        private Map<ListenerKey, ListenersObservers> listenersObservers = Collections.synchronizedMap(new HashMap<ListenerKey, ListenersObservers>());

        private Set<String> changedSubscriptions = new HashSet<String>();
        private Set<ListenerKey> changedListeners = new HashSet<ListenerKey>();
        private final Object changesLock = new Object();

        private ConnectStateWatcher connectStateWatcher = new ConnectStateWatcher(this);

        private Thread thread;

        private boolean stop;

        public void start() {
            stop = false;
            thread = new Thread(this, "RowLogConfigurationManager observers notifier");
            thread.start();
            zooKeeper.addDefaultWatcher(connectStateWatcher);
        }

        public synchronized void shutdown() throws InterruptedException {
            stop = true;
            zooKeeper.removeDefaultWatcher(connectStateWatcher);

            if (thread == null || !thread.isAlive()) {
                return;
            }

            thread.interrupt();
            thread.join();
            thread = null;
        }

        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    Set<String> changedSubscriptions;
                    Set<ListenerKey> changedListeners;

                    synchronized (changesLock) {
                        changedSubscriptions = new HashSet<String>(this.changedSubscriptions);
                        changedListeners = new HashSet<ListenerKey>(this.changedListeners);
                        this.changedSubscriptions.clear();
                        this.changedListeners.clear();
                    }

                    for (String rowLogId : changedSubscriptions) {
                        SubscriptionsObservers observers = subscriptionsObservers.get(rowLogId);
                        if (observers != null) {
                            boolean notify = observers.refresh();
                            if (notify)
                                observers.notifyObservers();
                        }
                    }

                    for (ListenerKey listenerKey : changedListeners) {
                        ListenersObservers observers = listenersObservers.get(listenerKey);
                        if (observers != null) {
                            boolean notify = observers.refresh();
                            if (notify)
                                observers.notifyObservers();
                        }
                    }

                    synchronized (changesLock) {
                        while (this.changedListeners.isEmpty() && this.changedSubscriptions.isEmpty()) {
                            changesLock.wait();
                        }
                    }
                } catch (KeeperException.ConnectionLossException e) {
                    // we will be retriggered when the connection is back
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    log.error("RowLogConfigurationManager observers notifier thread: some exception happened.", t);
                }
            }
        }

        public void notifySubscriptionsChanged(String rowLogId) {
            synchronized (changesLock) {
                changedSubscriptions.add(rowLogId);
                changesLock.notifyAll();
            }
        }

        public void notifyListenersChanged(ListenerKey listenerKey) {
            synchronized (changesLock) {
                changedListeners.add(listenerKey);
                changesLock.notifyAll();
            }
        }

        public void notifyEverythingChanged() {
            synchronized (changesLock) {
                for (String rowLogId : subscriptionsObservers.keySet()) {
                    changedSubscriptions.add(rowLogId);
                }
                for (ListenerKey listenerKey : listenersObservers.keySet()) {
                    changedListeners.add(listenerKey);
                }
                changesLock.notifyAll();
            }
        }

        public void addSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer) {
            synchronized (changesLock) {
                SubscriptionsObservers observers = subscriptionsObservers.get(rowLogId);
                if (observers == null) {
                    observers = new SubscriptionsObservers(rowLogId);
                    subscriptionsObservers.put(rowLogId, observers);
                }

                observers.add(observer);
            }

            // The below is to cause the newly registered observer to receive the initial list
            // of subscriptions. This will of course notify all observers, but this does not
            // really hurt and is simpler. Note that we do not want the observer to be called
            // from two different threads concurrently.
            notifySubscriptionsChanged(rowLogId);
        }

        public void removeSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer) {
            synchronized (changesLock) {
                SubscriptionsObservers observers = subscriptionsObservers.get(rowLogId);
                if (observers != null) {
                    observers.observers.remove(observer);

                    if (observers.observers.isEmpty()) {
                        subscriptionsObservers.remove(rowLogId);
                    }
                }
            }
        }

        void addListenersObserver(ListenerKey listenerKey, ListenersObserver observer) {
            synchronized (changesLock) {
                ListenersObservers observers = listenersObservers.get(listenerKey);
                if (observers == null) {
                    observers = new ListenersObservers(listenerKey);
                    listenersObservers.put(listenerKey, observers);
                }

                observers.add(observer);
            }

            // See comment over at addSubscriptionsObserver
            notifyListenersChanged(listenerKey);
        }

        public void removeListenersObserver(ListenerKey listenerKey, ListenersObserver observer) {
            synchronized (changesLock) {
                ListenersObservers observers = listenersObservers.get(listenerKey);
                if (observers != null) {
                    observers.observers.remove(observer);

                    if (observers.observers.isEmpty()) {
                        listenersObservers.remove(listenerKey);
                    }
                }
            }
        }

        private class SubscriptionsObservers {
            private String rowLogId;
            private SubscriptionsWatcher watcher;
            private Set<SubscriptionsObserver> observers = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<SubscriptionsObserver, Boolean>()));
            private List<SubscriptionContext> subscriptions = Collections.emptyList();

            public SubscriptionsObservers(String rowLogId) {
                this.rowLogId = rowLogId;
                watcher = new SubscriptionsWatcher(rowLogId, ObserverSupport.this);
            }

            public void add(SubscriptionsObserver observer) {
                this.observers.add(observer);
            }

            public boolean refresh() throws InterruptedException, KeeperException {
                List<SubscriptionContext> subscriptions = new ArrayList<SubscriptionContext>();

                List<String> subscriptionIds;
                Stat stat = new Stat();
                try {
                    subscriptionIds = zooKeeper.getChildren(subscriptionsPath(rowLogId), watcher, stat);
                } catch (NoNodeException exception) {
                    // Someone added an observer for a row log which does not exist, this is allowed
                    zooKeeper.exists(subscriptionsPath(rowLogId), watcher);
                    this.subscriptions = Collections.emptyList();
                    return true;
                }

                if (stat.getCversion() == 0) {
                    // When the node is initially created, do not trigger an event towards the observers.
                    // While this can do no harm to the observers, having no such extra event makes that we
                    // do not have to consider this implementation-detail in the testcases.
                    return false;
                }

                for (String subscriptionId : subscriptionIds) {
                    try {
                        byte[] data = zooKeeper.getData(subscriptionPath(rowLogId, subscriptionId), watcher, new Stat());
                        int maxTries = Bytes.toInt(data);
                        int orderNr = Bytes.toInt(data, Bytes.SIZEOF_INT);
                        Type type = Type.valueOf(Bytes.toString(data, Bytes.SIZEOF_INT * 2, data.length - (Bytes.SIZEOF_INT*2)));
                        subscriptions.add(new SubscriptionContext(subscriptionId, type, maxTries, orderNr));
                    } catch (NoNodeException e) {
                        // subscription was removed since the getChildren call, skip it
                    }
                }

                this.subscriptions = subscriptions;
                return true;
            }

            public void notifyObservers() {
                synchronized (observers) {
                    for (SubscriptionsObserver observer : observers) {
                        try {
                            // Note that if you enable this debug logging (or noticed this in some other way) you
                            // might see multiple times the same set of subscriptions being reported to observers.
                            // This is because each time a new observer is added, everyone observer is again called
                            // with the current set of subscriptions. See addSubscriptionObserver fo why this is.
                            // Since often a new observer is registered in response to the creation of a subscriptions,
                            // you will often see double events.
                            log.debug("Row log " + rowLogId + ": notifying to the observers " + observers +
                                    " that the current subscriptions are " + subscriptions);
                            observer.subscriptionsChanged(subscriptions);
                        } catch (Throwable t) {
                            log.error("Error notifying subscriptions observer " + observer.getClass().getName(), t);
                        }
                    }
                }
            }
        }

        private class ListenersObservers {
            private ListenerKey listenerKey;
            private ListenersWatcher watcher;
            private Set<ListenersObserver> observers = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<ListenersObserver, Boolean>()));
            private List<String> listeners = Collections.emptyList();

            public ListenersObservers(ListenerKey listenerKey) {
                this.listenerKey = listenerKey;
                watcher = new ListenersWatcher(listenerKey, ObserverSupport.this);
            }

            public void add(ListenersObserver observer) {
                this.observers.add(observer);
            }

            public boolean refresh() throws InterruptedException, KeeperException {
                Stat stat = new Stat();
                try {
                    listeners = zooKeeper.getChildren(subscriptionPath(listenerKey.rowLogId, listenerKey.subscriptionId), watcher, stat);
                    if (stat.getCversion() == 0) {
                        // When the node is initially created, do not trigger an event towards the observers.
                        // While this can do no harm to the observers, having no such extra event makes that we
                        // do not have to consider this implementation-detail in the testcases.
                        return false;
                    }
                } catch (NoNodeException exception) {
                    // someone added an observer for a {row log, subscription} which does not exist, this is allowed
                    zooKeeper.exists(subscriptionPath(listenerKey.rowLogId, listenerKey.subscriptionId), watcher);
                    listeners = Collections.emptyList();
                }
                return true;
            }

            public void notifyObservers() {
                synchronized (observers) {
                    for (ListenersObserver observer : observers) {
                        try {
                            log.debug("Row log " + listenerKey.rowLogId + ", subscription ID: " +
                                    listenerKey.subscriptionId + ": notifying to the observers " + observers +
                                    " that the current listeners are " + listeners);
                            observer.listenersChanged(listeners);
                        } catch (Throwable t) {
                            log.error("Error notifying listener observer " + observer.getClass().getName(), t);
                        }
                    }
                }
            }
        }

    }

    private static class ListenerKey {
        private String rowLogId;
        private String subscriptionId;

        public ListenerKey(String rowLogId, String subscriptionId) {
            ArgumentValidator.notNull(rowLogId, "rowLogId");
            ArgumentValidator.notNull(subscriptionId, "subscriptionId");
            this.rowLogId = rowLogId;
            this.subscriptionId = subscriptionId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ListenerKey other = (ListenerKey) obj;
            return other.rowLogId.equals(rowLogId) && other.subscriptionId.equals(subscriptionId);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + rowLogId.hashCode();
            result = prime * result + subscriptionId.hashCode();
            return result;
        }
    }

    // Watchers
    private class SubscriptionsWatcher implements Watcher {
        private final String rowLogId;
        private final ObserverSupport observerSupport;

        public SubscriptionsWatcher(String rowLogId, ObserverSupport observerSupport) {
            this.rowLogId = rowLogId;
            this.observerSupport = observerSupport;
        }
        
        public void process(WatchedEvent event) {
            if (!event.getType().equals(Watcher.Event.EventType.None)) {
                observerSupport.notifySubscriptionsChanged(rowLogId);
            }
        }
    }
    
    private class ListenersWatcher implements Watcher {
        private final ObserverSupport observerSupport;
        private final ListenerKey listenerKey;

        public ListenersWatcher(ListenerKey listenerKey, ObserverSupport observerSupport) {
            this.listenerKey = listenerKey;
            this.observerSupport = observerSupport;
        }
        
        public void process(WatchedEvent event) {
            if (!event.getType().equals(Watcher.Event.EventType.None)) {
                observerSupport.notifyListenersChanged(listenerKey);
            }
        }
    }

    public class ConnectStateWatcher implements Watcher {
        private final ObserverSupport observerSupport;

        public ConnectStateWatcher(ObserverSupport observerSupport) {
            this.observerSupport = observerSupport;
        }

        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                // Each time the connection is established, we trigger refreshing, since the previous refresh
                // might have failed with a ConnectionLoss exception
                observerSupport.notifyEverythingChanged();
            }
        }
    }

}
