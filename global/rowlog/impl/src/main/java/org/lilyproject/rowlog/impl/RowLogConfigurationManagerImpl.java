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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.rowlog.api.ListenersObserver;
import org.lilyproject.rowlog.api.ProcessorNotifyObserver;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogObserver;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.api.SubscriptionsObserver;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Logs;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;


// The paths used in zookeeper to store the data are :
// /lily/rowlog/<rowlogid>+<data>
// /lily/rowlog/<rowlogid>/shards/<shardid>/processorNotify+<data>
// /lily/rowlog/<rowlogid>/subscriptions/<subscriptionid>/<listenerid>

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
    
    // RowLogs
    public synchronized void addRowLog(String rowLogId, RowLogConfig rowLogConfig) throws KeeperException, InterruptedException {
        final String path = rowLogPath(rowLogId);
        
        final byte[] data = RowLogConfigConverter.INSTANCE.toJsonBytes(rowLogId, rowLogConfig);
        ZkUtil.createPath(zooKeeper, path, data, CreateMode.PERSISTENT);
        // Perform an extra update in case the rowlog node already existed
        zooKeeper.retryOperation(new ZooKeeperOperation<String>() {
            public String execute() throws KeeperException, InterruptedException {
                zooKeeper.setData(path, data, -1);
                return null;
            }
        });
    }
    
    public synchronized void updateRowLog(String rowLogId, RowLogConfig rowLogConfig) throws KeeperException, InterruptedException {
        final String path = rowLogPath(rowLogId);
        final byte[] data = RowLogConfigConverter.INSTANCE.toJsonBytes(rowLogId, rowLogConfig);
        zooKeeper.retryOperation(new ZooKeeperOperation<String>() {
            public String execute() throws KeeperException, InterruptedException {
                zooKeeper.setData(path, data, -1);
                return null;
            }
        });
    }

    public synchronized void removeRowLog(String rowLogId) throws KeeperException, InterruptedException, RowLogException {
        if (subscriptionsExist(rowLogId)) 
            throw new RowLogException("Cannot remove rowlog " + rowLogId + " because it has subscriptions ");
        if (!deepRemove(rowLogPath(rowLogId)))
            throw new RowLogException("Failed to remove rowlog " + rowLogId);
    }
    
    
    
    private synchronized boolean subscriptionsExist(String rowLogId) throws KeeperException, InterruptedException {
        final String subscriptionsPath = subscriptionsPath(rowLogId);
        return zooKeeper.retryOperation(new ZooKeeperOperation<Boolean>() {
           public Boolean execute() throws KeeperException, InterruptedException {
               return zooKeeper.exists(subscriptionsPath, false) != null;
           }
        });
    } 
    
    public boolean rowLogExists(String rowLogId) throws InterruptedException, KeeperException {
        final String path = rowLogPath(rowLogId);

        return zooKeeper.retryOperation(new ZooKeeperOperation<Boolean>() {
            public Boolean execute() throws KeeperException, InterruptedException {
                return zooKeeper.exists(path, false) != null;
            }
        });
    }
    
    public void addRowLogObserver(String rowLogId, RowLogObserver observer) {
        observerSupport.addRowLogObserver(rowLogId, observer);
    }
    
    public void removeRowLogObserver(String rowLogId, RowLogObserver observer) {
        observerSupport.removeRowLogObserver(rowLogId, observer);
    }
    
    // Subscriptions

    public void addSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer) {
        observerSupport.addSubscriptionsObserver(rowLogId, observer);
    }

    public void removeSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer) {
        observerSupport.removeSubscriptionsObserver(rowLogId, observer);
    }

    public boolean subscriptionExists(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException {
        final String path = subscriptionPath(rowLogId, subscriptionId);

        return zooKeeper.retryOperation(new ZooKeeperOperation<Boolean>() {
            public Boolean execute() throws KeeperException, InterruptedException {
                return zooKeeper.exists(path, false) != null;
            }
        });
    }

    public synchronized void addSubscription(String rowLogId, String subscriptionId, RowLogSubscription.Type type,
            int maxTries, int orderNr) throws KeeperException, InterruptedException, RowLogException {

        ZkUtil.createPath(zooKeeper, subscriptionsPath(rowLogId));

        final String path = subscriptionPath(rowLogId, subscriptionId);

        RowLogSubscription subscription = new RowLogSubscription(rowLogId, subscriptionId, type, maxTries, orderNr);
        final byte[] data = SubscriptionConverter.INSTANCE.toJsonBytes(subscription);

        try {
            zooKeeper.retryOperation(new ZooKeeperOperation<String>() {
                public String execute() throws KeeperException, InterruptedException {
                    return zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            });
        } catch (KeeperException.NodeExistsException e) {
            // The subscription already exists. This can be because someone else already created it, but also
            // because of the use of retryOperation.
            // We will try to update the subscription.
            updateSubscription(rowLogId, subscriptionId, type, maxTries, orderNr);
        }
    }
    
    public synchronized void updateSubscription(String rowLogId, String subscriptionId, RowLogSubscription.Type type, int maxTries, int orderNr) throws KeeperException, InterruptedException, RowLogException {
        if (!subscriptionExists(rowLogId, subscriptionId))
            throw new RowLogException("Subscription " + subscriptionId + "does not exist for rowlog " + rowLogId);
        
        final String path = subscriptionPath(rowLogId, subscriptionId);

        RowLogSubscription subscription = new RowLogSubscription(rowLogId, subscriptionId, type, maxTries, orderNr);
        final byte[] data = SubscriptionConverter.INSTANCE.toJsonBytes(subscription);

        try {
            zooKeeper.retryOperation(new ZooKeeperOperation<Object>() {
                public String execute() throws KeeperException, InterruptedException {
                    zooKeeper.setData(path, data, -1);
                    return null;
                }
            });
        } catch (KeeperException.NoNodeException e) {
            throw new RowLogException("Subscription " + subscriptionId + "does not exist for rowlog " + rowLogId);
        }
    }
    
    public synchronized void removeSubscription(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException, RowLogException {
        // The children of the subscription are ephemeral listener nodes. These listeners should shut
        // down when the subscription is removed. Usually it is the task of the application to shut
        // down the listeners before removing the subscription. If there would still be listeners running,
        // delete their ephemeral nodes now. A running listener will not do anything anymore, since it
        // will not be offered any message anymore.
        if (!deepRemove(subscriptionPath(rowLogId, subscriptionId)))
            throw new RowLogException("Failed to remove subscription " + subscriptionId + " from rowlog " + rowLogId);
    }
    
    private synchronized boolean deepRemove(final String path) throws InterruptedException, KeeperException {
        boolean success = false;
        int tryCount = 0;

        while (!success) {
            boolean removeChildren = false;
            try {
                zooKeeper.retryOperation(new ZooKeeperOperation<Object>() {
                    public Object execute() throws KeeperException, InterruptedException {
                        zooKeeper.delete(path, -1);
                        return null;
                    }
                });
                success = true;
            } catch (KeeperException.NoNodeException ignore) {
                // Silently ignore, might fail because of retryOperation
                success = true;
            } catch (KeeperException.NotEmptyException e) {
                removeChildren = true;
            }

            if (removeChildren) {
                zooKeeper.retryOperation(new ZooKeeperOperation<Object>() {
                    public Object execute() throws KeeperException, InterruptedException {
                        List<String> children;
                        try {
                            children = zooKeeper.getChildren(path, false);
                        } catch (NoNodeException e) {
                            // you never know
                            return null;
                        }
                        for (String child : children) {
                            try {
                                deepRemove(path + "/" + child);
                            } catch (NoNodeException e) {
                                // listener was removed in the meantime
                            }
                        }
                        return null;
                    }
                });
            }

            tryCount++;
            if (tryCount > 3) {
                return false;
            }
        }
        return true;
    }
    
    // Listeners

    public void addListenersObserver(String rowLogId, String subscriptionId, ListenersObserver observer) {
        observerSupport.addListenersObserver(new ListenerKey(rowLogId, subscriptionId), observer);
    }

    public void removeListenersObserver(String rowLogId, String subscriptionId, ListenersObserver observer) {
        observerSupport.removeListenersObserver(new ListenerKey(rowLogId, subscriptionId), observer);
    }

    public void addListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException,
            InterruptedException, KeeperException {
        final String path = listenerPath(rowLogId, subscriptionId, listenerId);
        try {
            zooKeeper.retryOperation(new ZooKeeperOperation<String>() {
                public String execute() throws KeeperException, InterruptedException {
                    return zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }
            });
        } catch (KeeperException.NoNodeException e) {
            // This is thrown when the parent does not exist
            throw new RowLogException("Cannot add listener: subscription does not exist. Row log ID " +
                    rowLogId + ", subscription ID " + subscriptionId + ", listener ID " + listenerId);
        } catch (KeeperException.NodeExistsException e) {
            // Silently ignore. Might occur because we use a retryOperation.
        }
    }
    
    public void removeListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException,
            InterruptedException, KeeperException {
        final String path = listenerPath(rowLogId, subscriptionId, listenerId);
        try {
            zooKeeper.retryOperation(new ZooKeeperOperation<Object>() {
                public Object execute() throws KeeperException, InterruptedException {
                    zooKeeper.delete(path, -1);
                    return null;
                }
            });
        } catch (KeeperException.NoNodeException ignore) {
            // Silently ignore. Might occur because we use retryOperation.
        }
    }
    
    // Processor Notify
    public void addProcessorNotifyObserver(String rowLogId, String shardId, ProcessorNotifyObserver observer) {
    	observerSupport.addProcessorNotifyObserver(new ProcessorKey(rowLogId, shardId), observer);
    }
    
    public void removeProcessorNotifyObserver(String rowLogId, String shardId) {
    	observerSupport.removeProcessorNotifyObserver(new ProcessorKey(rowLogId, shardId));
    }
    
    public void notifyProcessor(String rowLogId, String shardId) throws InterruptedException, KeeperException {
    	try {
    		zooKeeper.setData(processorNotifyPath(rowLogId, shardId), null, -1);
		} catch (KeeperException.NoNodeException e) {
			// No RowLogProcessor is listening
		}
    }
    
    // Paths
    private String rowLogPath(String rowLogId) {
        return rowLogPath + "/" + rowLogId;
    }
    
    private String subscriptionPath(String rowLogId, String subscriptionId) {
        return subscriptionsPath(rowLogId) + "/" + subscriptionId;
    }
    
    private String subscriptionsPath(String rowLogId) {
        return rowLogPath(rowLogId) + "/subscriptions";
    }
    
    private String shardPath(String rowLogId, String shardId) {
        return rowLogPath(rowLogId) + "/shards" + "/" + shardId;
    }
    
    private String processorNotifyPath(String rowLogId, String shardId) {
        return shardPath(rowLogId, shardId) + "/" + "processorNotify";
    }
    
    private String listenerPath(String rowLogId, String subscriptionId, String listenerId) {
        return subscriptionPath(rowLogId, subscriptionId) + "/" + listenerId;
    }
    

    private class ObserverSupport implements Runnable {
        /** key = row log id. */
        private Map<String, SubscriptionsObservers> subscriptionsObservers = Collections.synchronizedMap(new HashMap<String, SubscriptionsObservers>());
        private Map<ListenerKey, ListenersObservers> listenersObservers = Collections.synchronizedMap(new HashMap<ListenerKey, ListenersObservers>());
        private Map<ProcessorKey, ProcessorNotifyObserver> processorNotifyObservers = Collections.synchronizedMap(new HashMap<ProcessorKey, ProcessorNotifyObserver>());
        private Map<String, RowLogObservers> rowLogObservers = Collections.synchronizedMap(new HashMap<String, RowLogObservers>());
        
        private Set<String> changedSubscriptions = new HashSet<String>();
        private Set<ListenerKey> changedListeners = new HashSet<ListenerKey>();
        private Set<ProcessorKey> changedProcessors = new HashSet<ProcessorKey>();
        private Set<String> changedRowLogs = new HashSet<String>();
        private final Object changesLock = new Object();

        private ConnectStateWatcher connectStateWatcher = new ConnectStateWatcher(this);

        private Thread thread;

        private boolean stop; // do not rely only on Thread.interrupt since some libraries eat interruptions

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
            Logs.logThreadJoin(thread);
            thread.join();
            thread = null;
        }

        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    Set<String> changedSubscriptions;
                    Set<ListenerKey> changedListeners;
                    Set<ProcessorKey> changedProcessors;
                    Set<String> changedRowLogs;

                    synchronized (changesLock) {
                        changedSubscriptions = new HashSet<String>(this.changedSubscriptions);
                        changedListeners = new HashSet<ListenerKey>(this.changedListeners);
                        changedProcessors = new HashSet<ProcessorKey>(this.changedProcessors);
                        changedRowLogs = new HashSet<String>(this.changedRowLogs);
                        this.changedSubscriptions.clear();
                        this.changedListeners.clear();
                        this.changedProcessors.clear();
                        this.changedRowLogs.clear();
                    }
                    
                    for (String rowLogId : changedRowLogs) {
                        RowLogObservers observers = rowLogObservers.get(rowLogId);
                        if (observers != null) {
                            boolean notify = observers.refresh();
                            if (notify)
                                observers.notifyObservers();
                        }
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
                    
                    for (ProcessorKey processorKey : changedProcessors) {
                    	performNotifyProcessor(processorKey);
                    }

                    synchronized (changesLock) {
                        while (this.changedRowLogs.isEmpty() && this.changedListeners.isEmpty() && this.changedSubscriptions.isEmpty() && this.changedProcessors.isEmpty() && !stop) {
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
        
        public void notifyRowLogChanged(String rowLogId) {
            synchronized (changesLock) {
                changedRowLogs.add(rowLogId);
                changesLock.notifyAll();
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
        
        public void notifyProcessor(ProcessorKey processorKey) {
        	synchronized (changesLock) {
        		changedProcessors.add(processorKey);
        		changesLock.notifyAll();
        	}
        }

        private void performNotifyProcessor(ProcessorKey processorKey) throws InterruptedException, KeeperException {
        	ProcessorNotifyObserver processorNotifyObserver = processorNotifyObservers.get(processorKey);
        	if (processorNotifyObserver != null) {
        		processorNotifyObserver.notifyProcessor();
        		// Only put a watcher when an observer has been registerd
        		String processorNotifyPath = processorNotifyPath(processorKey.getRowLogId(), processorKey.getShardId());
        		try {
        			zooKeeper.getData(processorNotifyPath, new ProcessorNotifyWatcher(processorKey, ObserverSupport.this), null);
        		} catch (KeeperException.NoNodeException e) {
        			ZkUtil.createPath(zooKeeper, processorNotifyPath);
        			zooKeeper.getData(processorNotifyPath, new ProcessorNotifyWatcher(processorKey, ObserverSupport.this), null);
        		}
        	}
        }

        public void notifyEverythingChanged() {
            synchronized (changesLock) {
                for (String rowLogId : rowLogObservers.keySet()) {
                    changedRowLogs.add(rowLogId);
                }
                for (String rowLogId : subscriptionsObservers.keySet()) {
                    changedSubscriptions.add(rowLogId);
                }
                for (ListenerKey listenerKey : listenersObservers.keySet()) {
                    changedListeners.add(listenerKey);
                }
                for (ProcessorKey processorKey : processorNotifyObservers.keySet()) {
                	changedProcessors.add(processorKey);
                }
                changesLock.notifyAll();
            }
        }
        
        public void addRowLogObserver(String rowLogId, RowLogObserver observer) {
            synchronized (changesLock) {
                RowLogObservers observers = rowLogObservers.get(rowLogId);
                if (observers == null) {
                    observers = new RowLogObservers(rowLogId);
                    rowLogObservers.put(rowLogId, observers);
                }
                observers.add(observer);
            }
            // The below is to cause the newly registered observer to receive the initial
            // rowLogConfig. This will of course notify all observers, but this does not
            // really hurt and is simpler. Note that we do not want the observer to be called
            // from two different threads concurrently.
            notifyRowLogChanged(rowLogId);
        }

        public void removeRowLogObserver(String rowLogId, RowLogObserver observer) {
            synchronized (changesLock) {
                RowLogObservers observers = rowLogObservers.get(rowLogId);
                if (observers != null) {
                    observers.observers.remove(observer);
                    
                    if (observers.observers.isEmpty()) {
                        rowLogObservers.remove(rowLogId);
                    }
                }
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
        
        void addProcessorNotifyObserver(ProcessorKey processorKey, ProcessorNotifyObserver observer) {
        	synchronized(changesLock) {
        		processorNotifyObservers.put(processorKey, observer);
        	}
        	notifyProcessor(processorKey);
        }
        
        void removeProcessorNotifyObserver(ProcessorKey processorKey) {
        	synchronized(changesLock) {
        		processorNotifyObservers.remove(processorKey);
        	}
        }
        
        private class RowLogObservers {
            private String rowLogId;
            private RowLogWatcher watcher;
            private Set<RowLogObserver> observers = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<RowLogObserver, Boolean>()));
            private RowLogConfig config = null;

            public RowLogObservers(String rowLogId) {
                this.rowLogId = rowLogId;
                watcher = new RowLogWatcher(rowLogId, ObserverSupport.this);
            }

            public void add(RowLogObserver observer) {
                this.observers.add(observer);
            }

            public boolean refresh() throws InterruptedException, KeeperException {
                Stat stat = new Stat();
                try {
                    byte[] data = zooKeeper.getData(rowLogPath(rowLogId), watcher, stat);
                    if (data == null) 
                        return false;
                    config = RowLogConfigConverter.INSTANCE.fromJsonBytes(rowLogId, data);
                } catch (NoNodeException e) {
                 // Someone added an observer for a row log which does not exist, this is allowed
                    zooKeeper.exists(rowLogPath(rowLogId), watcher);
                    config = null;
                }
                
                return true;
            }

            public void notifyObservers() {
                for (RowLogObserver observer : new ArrayList<RowLogObserver>(observers)) {
                    try {
                        // Note that if you enable this debug logging (or noticed this in some other way) you
                        // might see multiple times the same rowlog config being reported to observers.
                        // This is because each time a new observer is added, everyone observer is again called
                        // with the current rowlog config. See addRowLogObserver for why this is.
                        // Since often a new observer is registered in response to a configuration change,
                        // you will often see double events.
                        log.debug("Row log " + rowLogId + ": notifying to the observers " + observers +
                                " that the current config is" + config);
                        observer.rowLogConfigChanged(config);
                    } catch (Throwable t) {
                        log.error("Error notifying rowlog observer " + observer.getClass().getName(), t);
                    }
                }
            }
        }

        private class SubscriptionsObservers {
            private String rowLogId;
            private SubscriptionsWatcher watcher;
            private Set<SubscriptionsObserver> observers = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<SubscriptionsObserver, Boolean>()));
            private List<RowLogSubscription> subscriptions = Collections.emptyList();

            public SubscriptionsObservers(String rowLogId) {
                this.rowLogId = rowLogId;
                watcher = new SubscriptionsWatcher(rowLogId, ObserverSupport.this);
            }

            public void add(SubscriptionsObserver observer) {
                this.observers.add(observer);
            }

            public boolean refresh() throws InterruptedException, KeeperException {
                List<RowLogSubscription> subscriptions = new ArrayList<RowLogSubscription>();

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
                        RowLogSubscription subscription = SubscriptionConverter.INSTANCE.fromJsonBytes(rowLogId, subscriptionId, data);
                        subscriptions.add(subscription);
                    } catch (NoNodeException e) {
                        // subscription was removed since the getChildren call, skip it
                    }
                }

                this.subscriptions = subscriptions;
                return true;
            }

            public void notifyObservers() {
                for (SubscriptionsObserver observer : new ArrayList<SubscriptionsObserver>(observers)) {
                    try {
                        // Note that if you enable this debug logging (or noticed this in some other way) you
                        // might see multiple times the same set of subscriptions being reported to observers.
                        // This is because each time a new observer is added, everyone observer is again called
                        // with the current set of subscriptions. See addSubscriptionObserver for why this is.
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
    
    private static class ProcessorKey {
    	private String rowLogId;
    	private String shardId;
    	
    	public ProcessorKey(String rowLogId, String shardId) {
    		ArgumentValidator.notNull(rowLogId, "rowLogId");
    		ArgumentValidator.notNull(shardId, "shardId");
    		this.rowLogId = rowLogId;
    		this.shardId = shardId;
    	}
    	
    	public String getRowLogId() {
    		return rowLogId;
    	}
    	
    	public String getShardId() {
    		return shardId;
    	}
    	
    	@Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ProcessorKey other = (ProcessorKey) obj;
            return other.rowLogId.equals(rowLogId) && other.shardId.equals(shardId);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + rowLogId.hashCode();
            result = prime * result + shardId.hashCode();
            return result;
        }
    }

    // Watchers
    private class RowLogWatcher implements Watcher {
        private final String rowLogId;
        private final ObserverSupport observerSupport;
        
        public RowLogWatcher(String rowLogId, ObserverSupport observerSupport) {
            this.rowLogId = rowLogId;
            this.observerSupport = observerSupport;
        }
        
        public void process(WatchedEvent event) {
            EventType eventType = event.getType(); 
            if (!eventType.equals(Watcher.Event.EventType.None) && !eventType.equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                observerSupport.notifyRowLogChanged(rowLogId);
            }
        }
    }
    
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
    	private final ListenerKey listenerKey;
        private final ObserverSupport observerSupport;

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
    
    private class ProcessorNotifyWatcher implements Watcher {
    	private final ProcessorKey processorKey;
    	private final ObserverSupport observerSupport;
    	
    	public ProcessorNotifyWatcher(ProcessorKey processorKey, ObserverSupport observerSupport) {
    		this.processorKey = processorKey;
    		this.observerSupport = observerSupport;
    	}
    	
    	public void process(WatchedEvent event) {
    		if (!event.getType().equals(Watcher.Event.EventType.None)) {
    			observerSupport.notifyProcessor(processorKey);
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
