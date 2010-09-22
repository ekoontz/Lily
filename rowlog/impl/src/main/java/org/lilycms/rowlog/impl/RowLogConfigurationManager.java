package org.lilycms.rowlog.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.Stat;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.SubscriptionContext;
import org.lilycms.rowlog.api.SubscriptionContext.Type;
import org.lilycms.util.zookeeper.ZkConnectException;
import org.lilycms.util.zookeeper.ZkPathCreationException;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

public class RowLogConfigurationManager {
    private Log log = LogFactory.getLog(getClass());
    private String lilyPath = "/lily";
    private String rowLogPath = lilyPath + "/rowlog";
    
    private ZooKeeperItf zooKeeper;
    
    @Override
    protected void finalize() throws Throwable {
        stop();
        super.finalize();
    }
    
    public void stop() throws InterruptedException {
        if (zooKeeper != null) {
            long sessionId = zooKeeper.getSessionId();
            zooKeeper.close();
            log.info("Closed zookeeper connection with sessionId 0x"+Long.toHexString(sessionId));
        }
    }
    
    public RowLogConfigurationManager(Configuration configuration) throws RowLogException {
        try {
            int sessionTimeout = configuration.getInt("zookeeper.session.timeout", 60 * 1000);
            String connectString = configuration.get("hbase.zookeeper.quorum") + ":" + configuration.get("hbase.zookeeper.property.clientPort");
            zooKeeper = ZkUtil.connect(connectString, sessionTimeout);
        } catch (ZkConnectException e) {
            throw new RowLogException("Failed to instantiate RowLogConfigurationManager", e);
        }
    }
    
    // Subscriptions
    public List<SubscriptionContext> getAndMonitorSubscriptions(RowLogProcessor processor, RowLog rowLog) throws KeeperException, InterruptedException {
        List<SubscriptionContext> subscriptions = new ArrayList<SubscriptionContext>();
        try {
            String rowLogId = rowLog.getId();
            List<String> subscriptionIds = zooKeeper.getChildren(subscriptionsPath(rowLogId), new SubscriptionsWatcher(processor, rowLog));
            for (String subscriptionId : subscriptionIds) {
                byte[] data = zooKeeper.getData(subscriptionPath(rowLogId, Integer.valueOf(subscriptionId)), false, new Stat());
                String dataString = Bytes.toString(data);
                String[] splitData = dataString.split(",");
                Type type = Type.valueOf(splitData[0]);
                int workerCount = Integer.valueOf(splitData[1]);
                subscriptions.add(new SubscriptionContext(Integer.valueOf(subscriptionId), type, workerCount));
            }
        } catch (NoNodeException exception) {
            // TODO Do we need to put another watcher here? How to cope with non-existing paths? Make them mandatory?
        } catch (SessionExpiredException exception) {
            // TODO ok to ignore this? Should I rather throw an exception
        }
        return subscriptions;
    }
    
    public void addSubscription(String rowLogId, int subscriptionId, SubscriptionContext.Type type, int workerCount) throws KeeperException, InterruptedException {
        String path = subscriptionPath(rowLogId, subscriptionId);
        String dataString = type.name() + "," + workerCount;
        byte[] data = Bytes.toBytes(dataString);
        if (zooKeeper.exists(path, false) == null) { // TODO currently not possible to update a subscription or add it twice
            try {
                ZkUtil.createPath(zooKeeper, path, data, CreateMode.PERSISTENT);
            } catch (ZkPathCreationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    public void removeSubscription(String rowLogId, int subscriptionId) throws InterruptedException, KeeperException {
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
    public List<String> getAndMonitorListeners(ListenersWatcherCallBack callBack, String rowLogId, int subscriptionId) throws KeeperException, InterruptedException {
        List<String> listeners = new ArrayList<String>();
        try {
            return zooKeeper.getChildren(subscriptionPath(rowLogId, subscriptionId), new ListenersWatcher(callBack, rowLogId, subscriptionId));
        } catch (NoNodeException exception) {
            // TODO Do we need to put another watcher here? How to cope with non-existing paths? Make them mandatory?
        } catch (SessionExpiredException exception) {
            // TODO ok to ignore this? Should I rather throw an exception
        }
        return listeners;
    }
    
    public void addListener(String rowLogId, int subscriptionId, String listenerId) throws RowLogException {
        String path = listenerPath(rowLogId, subscriptionId, listenerId);
        try {
            if (zooKeeper.exists(path, false) == null) {
                ZkUtil.createPath(zooKeeper, path, null, CreateMode.EPHEMERAL);
            }
        } catch (Exception e) {
            throw new RowLogException("Failed to add listener to rowlog configuration", e);
        }
    }
    
    public void removeListener(String rowLogId, int subscriptionId, String listenerId) throws RowLogException {
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
    private String subscriptionPath(String rowLogId, int subscriptionId) {
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
    
    private String listenerPath(String rowLogId, int subscriptionId, String listenerId) {
        return subscriptionPath(rowLogId, subscriptionId) + "/" + listenerId;
    }
    

    // Watchers
    private class SubscriptionsWatcher implements Watcher {
        private final RowLogProcessor processor;
        private final RowLog rowLog;

        public SubscriptionsWatcher(RowLogProcessor processor, RowLog rowLog) {
            this.processor = processor;
            this.rowLog = rowLog;
        }
        
        public void process(WatchedEvent event) {
            try {
                if (event.getState() == Event.KeeperState.Disconnected)
                    return;
                if (event.getState() == Event.KeeperState.Expired)
                    return;
                processor.subscriptionsChanged(getAndMonitorSubscriptions(processor, rowLog));
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
        private final int subscriptionId;
        private final ListenersWatcherCallBack callBack;

        public ListenersWatcher(ListenersWatcherCallBack callBack, String rowLogId, int subscriptionId) {
            this.callBack = callBack;
            this.rowLogId = rowLogId;
            this.subscriptionId = subscriptionId;
        }
        
        public void process(WatchedEvent event) {
            try {
                if (event.getState() == Event.KeeperState.Disconnected)
                    return;
                if (event.getState() == Event.KeeperState.Expired)
                    return;
                callBack.listenersChanged(getAndMonitorListeners(callBack, rowLogId, subscriptionId));
            } catch (KeeperException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    private class ZkWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }
}
