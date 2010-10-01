package org.lilycms.rowlog.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.Stat;
import org.lilycms.rowlog.api.RowLogConfigurationManager;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.SubscriptionContext;
import org.lilycms.rowlog.api.SubscriptionContext.Type;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

public class RowLogConfigurationManagerImpl implements RowLogConfigurationManager {
    private String lilyPath = "/lily";
    private String rowLogPath = lilyPath + "/rowlog";
    
    private ZooKeeperItf zooKeeper;
    
    public RowLogConfigurationManagerImpl(ZooKeeperItf zooKeeper) throws RowLogException {
        this.zooKeeper = zooKeeper;
    }
    
    // Subscriptions
    public synchronized List<SubscriptionContext> getAndMonitorSubscriptions(String rowLogId, SubscriptionsWatcherCallBack callBack) throws KeeperException, InterruptedException {
        List<SubscriptionContext> subscriptions = new ArrayList<SubscriptionContext>();
        try {
            List<String> subscriptionIds = zooKeeper.getChildren(subscriptionsPath(rowLogId), new SubscriptionsWatcher(rowLogId, callBack));
            for (String subscriptionId : subscriptionIds) {
                byte[] data = zooKeeper.getData(subscriptionPath(rowLogId, subscriptionId), false, new Stat());
                int maxTries = Bytes.toInt(data);
                int orderNr = Bytes.toInt(data, Bytes.SIZEOF_INT);
                Type type = Type.valueOf(Bytes.toString(data, Bytes.SIZEOF_INT * 2, data.length - (Bytes.SIZEOF_INT*2)));
                subscriptions.add(new SubscriptionContext(subscriptionId, type, maxTries, orderNr));
            }
        } catch (NoNodeException exception) {
            zooKeeper.exists(subscriptionsPath(rowLogId), new SubscriptionsWatcher(rowLogId, callBack));
            // TODO Do we need to put another watcher here? How to cope with non-existing paths? Make them mandatory?
        } catch (SessionExpiredException exception) {
            // TODO ok to ignore this? Should I rather throw an exception
        }
        return subscriptions;
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
    public List<String> getAndMonitorListeners(String rowLogId, String subscriptionId, ListenersWatcherCallBack callBack) throws KeeperException, InterruptedException {
        List<String> listeners = new ArrayList<String>();
        try {
            return zooKeeper.getChildren(subscriptionPath(rowLogId, subscriptionId), new ListenersWatcher(rowLogId, subscriptionId, callBack));
        } catch (NoNodeException exception) {
            zooKeeper.exists(subscriptionPath(rowLogId, subscriptionId), new ListenersWatcher(rowLogId, subscriptionId, callBack));
        } catch (SessionExpiredException exception) {
            // TODO ok to ignore this? Should I rather throw an exception
        }
        return listeners;
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
    

    // Watchers
    private class SubscriptionsWatcher implements Watcher {
        private final SubscriptionsWatcherCallBack callBack;
        private final String rowLogId;

        public SubscriptionsWatcher(String rowLogId, SubscriptionsWatcherCallBack callBack) {
            this.rowLogId = rowLogId;
            this.callBack = callBack;
        }
        
        public void process(WatchedEvent event) {
            try {
                if (event.getState() == Event.KeeperState.Disconnected)
                    return;
                if (event.getState() == Event.KeeperState.Expired)
                    return;
                callBack.subscriptionsChanged(getAndMonitorSubscriptions(rowLogId, callBack));
            } catch (KeeperException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    private class ListenersWatcher implements Watcher {

        private final String rowLogId;
        private final String subscriptionId;
        private final ListenersWatcherCallBack callBack;

        public ListenersWatcher(String rowLogId, String subscriptionId, ListenersWatcherCallBack callBack) {
            this.rowLogId = rowLogId;
            this.subscriptionId = subscriptionId;
            this.callBack = callBack;
        }
        
        public void process(WatchedEvent event) {
            try {
                if (event.getState() == Event.KeeperState.Disconnected)
                    return;
                if (event.getState() == Event.KeeperState.Expired)
                    return;
                callBack.listenersChanged(getAndMonitorListeners(rowLogId, subscriptionId, callBack));
            } catch (KeeperException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
