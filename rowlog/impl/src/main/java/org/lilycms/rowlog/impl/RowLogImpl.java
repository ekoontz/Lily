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
package org.lilycms.rowlog.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.*;

/**
 * See {@link RowLog}
 */
public class RowLogImpl implements RowLog, SubscriptionsObserver {

    private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");
    private RowLogShard shard; // TODO: We only work with one shard for now
    private final HTableInterface rowTable;
    private final byte[] payloadColumnFamily;
    private final byte[] executionStateColumnFamily;
    
    private Map<String, RowLogSubscription> subscriptions = Collections.synchronizedMap(new HashMap<String, RowLogSubscription>());
    private final long lockTimeout;
    private final String id;
    private RowLogProcessorNotifier processorNotifier = null;
    private Log log = LogFactory.getLog(getClass());
    private final boolean respectOrder;
    private RowLogConfigurationManager rowLogConfigurationManager;
    
    /**
     * The RowLog should be instantiated with information about the table that contains the rows the messages are 
     * related to, and the column families it can use within this table to put the payload and execution state of the
     * messages on.
     * @param rowTable the HBase table containing the rows to which the messages are related
     * @param payloadColumnFamily the column family in which the payload of the messages can be stored
     * @param executionStateColumnFamily the column family in which the execution state of the messages can be stored
     * @param lockTimeout the timeout to be used for the locks that are put on the messages
     * @param respectOrder true if the order of the subscriptions needs to be followed
     * @throws RowLogException
     */
    public RowLogImpl(String id, HTableInterface rowTable, byte[] payloadColumnFamily, byte[] executionStateColumnFamily,
            long lockTimeout, boolean respectOrder, RowLogConfigurationManager rowLogConfigurationManager) throws RowLogException {
        this.id = id;
        this.rowTable = rowTable;
        this.payloadColumnFamily = payloadColumnFamily;
        this.executionStateColumnFamily = executionStateColumnFamily;
        this.lockTimeout = lockTimeout;
        this.respectOrder = respectOrder;
        this.processorNotifier = new RowLogProcessorNotifier(rowLogConfigurationManager);
        this.rowLogConfigurationManager = rowLogConfigurationManager;
        rowLogConfigurationManager.addSubscriptionsObserver(id, this);
    }

    public void stop() {
        rowLogConfigurationManager.removeSubscriptionsObserver(id, this);
        if (processorNotifier != null) {
            processorNotifier.close();
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        stop();
        super.finalize();
    }
    
    public String getId() {
        return id;
    }
    
    public void registerShard(RowLogShard shard) {
        this.shard = shard;
    }
    
    public void unRegisterShard(RowLogShard shard) {
        this.shard = null;
    }
    
    private long putPayload(byte[] rowKey, byte[] payload, Put put) throws IOException {
        Get get = new Get(rowKey);
        get.addColumn(payloadColumnFamily, SEQ_NR);
        Result result = rowTable.get(get);
        byte[] value = result.getValue(payloadColumnFamily, SEQ_NR);
        long seqnr = -1;
        if (value != null) {
            seqnr = Bytes.toLong(value);
        }
        seqnr++;
        if (put != null) {
            put.add(payloadColumnFamily, SEQ_NR, Bytes.toBytes(seqnr));
            put.add(payloadColumnFamily, Bytes.toBytes(seqnr), payload);
        } else {
            put = new Put(rowKey);
            put.add(payloadColumnFamily, SEQ_NR, Bytes.toBytes(seqnr));
            put.add(payloadColumnFamily, Bytes.toBytes(seqnr), payload);
            rowTable.put(put);
        }
        return seqnr;
    }

    public byte[] getPayload(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        Get get = new Get(rowKey);
        get.addColumn(payloadColumnFamily, Bytes.toBytes(seqnr));
        Result result;
        try {
            result = rowTable.get(get);
        } catch (IOException e) {
            throw new RowLogException("Failed to get payload from the rowTable", e);
        }
        return result.getValue(payloadColumnFamily, Bytes.toBytes(seqnr));
    }

    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException {
        RowLogShard shard = getShard(); // Fail fast if no shards are registered
        
        try {
            long seqnr = putPayload(rowKey, payload, put);
        
            byte[] messageId = Bytes.toBytes(System.currentTimeMillis()); 
            messageId = Bytes.add(messageId, Bytes.toBytes(seqnr));
            messageId = Bytes.add(messageId, rowKey);
            
            RowLogMessage message = new RowLogMessageImpl(messageId, rowKey, seqnr, data, this);
        
            synchronized(subscriptions) {
                shard.putMessage(message);
                initializeSubscriptions(message, put);
            }
            if (processorNotifier != null) {
                processorNotifier.notifyProcessor(id, shard.getId());
            }
            return message;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLog", e);
        }
    }

    
    private void initializeSubscriptions(RowLogMessage message, Put put) throws IOException {
        SubscriptionExecutionState executionState = new SubscriptionExecutionState(message.getId());
        synchronized (subscriptions) {
            for (RowLogSubscription subscription : subscriptions.values()) {
                executionState.setState(subscription.getId(), false);
            }
        }
        if (put != null) {
            put.add(executionStateColumnFamily, Bytes.toBytes(message.getSeqNr()), executionState.toBytes());
        } else {
            put = new Put(message.getRowKey());
            put.add(executionStateColumnFamily, Bytes.toBytes(message.getSeqNr()), executionState.toBytes());
            rowTable.put(put);
        }
    }

    public boolean processMessage(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
            Get get = new Get(rowKey);
            get.addColumn(executionStateColumnFamily, qualifier);
            try {
                Result result = rowTable.get(get);
                byte[] previousValue = result.getValue(executionStateColumnFamily, qualifier);
                SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);

                boolean allDone = processMessage(message, executionState);
                
                if (allDone) {
                    return removeExecutionStateAndPayload(rowKey, qualifier, previousValue);
                } else {
                    updateExecutionState(rowKey, qualifier, executionState, previousValue);
                    return false;
                }
            } catch (IOException e) {
                throw new RowLogException("Failed to process message", e);
            }
    }

    private boolean processMessage(RowLogMessage message, SubscriptionExecutionState executionState) throws RowLogException {
        boolean allDone = true;
        List<RowLogSubscription> subscriptionsSnapshot = getSubscriptions();
        if (respectOrder) {
            Collections.sort(subscriptionsSnapshot);
        }
        for (RowLogSubscription subscription : getSubscriptions()) {
            String subscriptionId = subscription.getId();
            if (!executionState.getState(subscriptionId)) {
                boolean done = false;
                executionState.incTryCount(subscriptionId);
                try {
                    RowLogMessageListener listener = RowLogMessageListenerMapping.INSTANCE.get(subscriptionId);
                    if (listener != null) 
                        done = listener.processMessage(message);
                } catch (Throwable t) {
                    executionState.setState(subscriptionId, false);
                    return false;
                }
                executionState.setState(subscriptionId, done);
                if (!done) {
                    allDone = false;
                    checkAndMarkProblematic(message, subscription, executionState);
                    if (respectOrder) {
                        break;
                    }
                } else {
                    shard.removeMessage(message, subscriptionId);
                }
            }
        }
        return allDone;
    }
    
    public List<RowLogSubscription> getSubscriptions() {
        synchronized (subscriptions) {
            return new ArrayList<RowLogSubscription>(subscriptions.values());
        }
    }
    
    public byte[] lockMessage(RowLogMessage message, String subscriptionId) throws RowLogException {
        return lockMessage(message, subscriptionId, 0);
    }
    
    private byte[] lockMessage(RowLogMessage message, String subscriptionId, int count) throws RowLogException {
        if (count >= 10) {
            return null;
        }
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        try {
            Result result = rowTable.get(get);
            if (result.isEmpty()) {
                return null;
            }
            byte[] previousValue = result.getValue(executionStateColumnFamily, qualifier);
            SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);
            byte[] previousLock = executionState.getLock(subscriptionId);
            executionState.incTryCount(subscriptionId);
            long now = System.currentTimeMillis();
            if (previousLock == null) {
                return putLock(message, subscriptionId, rowKey, qualifier, previousValue, executionState, now, count);
            } else {
                long previousTimestamp = Bytes.toLong(previousLock);
                if (previousTimestamp + lockTimeout < now) {
                    return putLock(message, subscriptionId, rowKey, qualifier, previousValue, executionState, now, count);
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to lock message", e);
        }
    }

    private byte[] putLock(RowLogMessage message, String subscriptionId, byte[] rowKey, byte[] qualifier, byte[] previousValue,
            SubscriptionExecutionState executionState, long now, int count) throws RowLogException {
        byte[] lock = Bytes.toBytes(now);
        executionState.setLock(subscriptionId, lock);
        Put put = new Put(rowKey);
        put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
        try {
            if (!rowTable.checkAndPut(rowKey, executionStateColumnFamily, qualifier, previousValue, put)) {
                return lockMessage(message, subscriptionId, count+1); // Retry
            } else {
                return lock;
            }
        } catch (IOException e) {
            return lockMessage(message, subscriptionId, count+1); // Retry
        }
    }
    
    public boolean unlockMessage(RowLogMessage message, String subscriptionId, boolean realTry, byte[] lock) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        Result result;
        try {
            result = rowTable.get(get);
            
            if (result.isEmpty()) return false; // The execution state does not exist anymore, thus no lock to unlock
            
            byte[] previousValue = result.getValue(executionStateColumnFamily, qualifier);
            SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);
            byte[] previousLock = executionState.getLock(subscriptionId);
            if (!Bytes.equals(lock, previousLock)) return false; // The lock was lost  
            
            executionState.setLock(subscriptionId, null);
            if (realTry) {
                checkAndMarkProblematic(message, subscriptions.get(subscriptionId), executionState);
            } else { 
                executionState.decTryCount(subscriptionId);
            }
            Put put = new Put(rowKey);
            put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
            return rowTable.checkAndPut(rowKey, executionStateColumnFamily, qualifier, previousValue, put);
        } catch (IOException e) {
            throw new RowLogException("Failed to unlock message", e);
        }
    }

    private void checkAndMarkProblematic(RowLogMessage message, RowLogSubscription subscription,
            SubscriptionExecutionState executionState) throws RowLogException {
        int maxTries = subscription.getMaxTries();
        RowLogShard rowLogShard = getShard();
        if (executionState.getTryCount(subscription.getId()) >= maxTries) {
            if (respectOrder) {
                List<RowLogSubscription> subscriptionContexts = getSubscriptions();
                Collections.sort(subscriptionContexts);
                for (RowLogSubscription subscriptionContext : subscriptionContexts) {
                    if (subscriptionContext.getOrderNr() >= subscription.getOrderNr()) {
                        rowLogShard.markProblematic(message, subscriptionContext.getId());
                        log.warn(String.format("Subscription %1$s failed to process message %2$s %3$s times, it has been marked as problematic", subscriptionContext.getId(), message.getId(), maxTries));
                    }
                }
            } else {
                getShard().markProblematic(message, subscription.getId());
            }
            log.warn(String.format("Subscription %1$s failed to process message %2$s %3$s times, it has been marked as problematic", subscription.getId(), message.getId(), maxTries));
        }
    }
    
    public boolean isMessageLocked(RowLogMessage message, String subscriptionId) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        try {
            Result result = rowTable.get(get);
            if (result.isEmpty()) return false;
            
            SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(result.getValue(executionStateColumnFamily, qualifier));
            byte[] lock = executionState.getLock(subscriptionId);
            if (lock == null) return false;
        
            return (Bytes.toLong(lock) + lockTimeout > System.currentTimeMillis());
        } catch (IOException e) {
            throw new RowLogException("Failed to check if messages is locked", e);
        }
    }
    
    public boolean messageDone(RowLogMessage message, String subscriptionId, byte[] lock) throws RowLogException {
        return messageDone(message, subscriptionId, lock, 0);
    }
    
    private boolean messageDone(RowLogMessage message, String subscriptionId, byte[] lock, int count) throws RowLogException {
        if (count >= 10) {
            return false;
        }
        RowLogShard shard = getShard(); // Fail fast if no shards are registered
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        try {
            Result result = rowTable.get(get);
            if (!result.isEmpty()) {
                byte[] previousValue = result.getValue(executionStateColumnFamily, qualifier);
                SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(previousValue);
                if (!Bytes.equals(lock,executionState.getLock(subscriptionId))) {
                    return false; // Not owning the lock
                }
                executionState.setState(subscriptionId, true);
                executionState.setLock(subscriptionId, null);
                if (executionState.allDone()) {
                    removeExecutionStateAndPayload(rowKey, qualifier, previousValue);
                } else {
                    if (!updateExecutionState(rowKey, qualifier, executionState, previousValue)) {
                        return messageDone(message, subscriptionId, lock, count+1); // Retry
                    }
                }
            }
            shard.removeMessage(message, subscriptionId);
            return true;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message to done", e);
        }
    }
    
    public boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        SubscriptionExecutionState executionState = getExecutionState(message);
        if (executionState == null)
            return true;
        return executionState.getState(subscriptionId);
    }

    private SubscriptionExecutionState getExecutionState(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        SubscriptionExecutionState executionState = null;
        try {
            Result result = rowTable.get(get);
            byte[] previousValue = result.getValue(executionStateColumnFamily, qualifier);
            if (previousValue != null)
                executionState = SubscriptionExecutionState.fromBytes(previousValue);
        } catch (IOException e) {
            throw new RowLogException("Failed to check if message is done", e);
        }
        return executionState;
    }

    public boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException {
        SubscriptionExecutionState executionState = getExecutionState(message);
        if (executionState == null)
            return false;
        if (respectOrder) {
            List<RowLogSubscription> subscriptionsContexts = getSubscriptions();
            Collections.sort(subscriptionsContexts);
            for (RowLogSubscription subscriptionContext : subscriptionsContexts) {
                if (subscriptionId.equals(subscriptionContext.getId()))
                    break;
                if (!executionState.getState(subscriptionContext.getId())) {
                    return false; // There is a previous subscription to be processed first
                }
            }
        }
        return !executionState.getState(subscriptionId);
    }
    
    private boolean updateExecutionState(byte[] rowKey, byte[] qualifier, SubscriptionExecutionState executionState, byte[] previousValue) throws IOException {
        Put put = new Put(rowKey);
        put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
        return rowTable.checkAndPut(rowKey, executionStateColumnFamily, qualifier, previousValue, put);
    }

    private boolean removeExecutionStateAndPayload(byte[] rowKey, byte[] qualifier, byte[] previousValue) throws IOException {
        Delete delete = new Delete(rowKey); 
        delete.deleteColumns(executionStateColumnFamily, qualifier);
        delete.deleteColumns(payloadColumnFamily, qualifier);
        return rowTable.checkAndDelete(rowKey, executionStateColumnFamily, qualifier, previousValue, delete);
    }

    
    // For now we work with only one shard
    private RowLogShard getShard() throws RowLogException {
        if (shard == null) {
            throw new RowLogException("No shards registerd");
        }
        return shard;
    }

    public List<RowLogMessage> getMessages(byte[] rowKey, String ... subscriptionIds) throws RowLogException {
        List<RowLogMessage> messages = new ArrayList<RowLogMessage>();
        Get get = new Get(rowKey);
        get.addFamily(executionStateColumnFamily);
        try {
            Result result = rowTable.get(get);
            if (!result.isEmpty()) {
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(executionStateColumnFamily);
                for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(entry.getValue());
                    boolean add = false;
                    if (subscriptionIds.length == 0)
                        add = true;
                    else {
                        for (String subscriptionId : subscriptionIds) {
                            if (!executionState.getState(subscriptionId))
                                add = true;
                        }
                    }
                    if (add)
                        messages.add(new RowLogMessageImpl(executionState.getMessageId(), rowKey, Bytes.toLong(entry.getKey()), null, this));
                }
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to get messages", e);
        }
        return messages;
    }
    
    public List<RowLogMessage> getProblematic(String subscriptionId) throws RowLogException {
        return getShard().getProblematic(subscriptionId);
    }
    
    public boolean isProblematic(RowLogMessage message, String subscriptionId) throws RowLogException {
        return getShard().isProblematic(message, subscriptionId);
    }
    
    public void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
        synchronized (subscriptions) {
            for (RowLogSubscription subscription : newSubscriptions) {
                if (!subscriptions.containsKey(subscription.getId()))
                    subscriptions.put(subscription.getId(), subscription);
            }
            Iterator<RowLogSubscription> iterator = subscriptions.values().iterator();
            while (iterator.hasNext()) {
                RowLogSubscription subscription = iterator.next();
                if (!newSubscriptions.contains(subscription))
                    iterator.remove();
            }
        }
    }

    public List<RowLogShard> getShards() {
        List<RowLogShard> shards = new ArrayList<RowLogShard>();
        shards.add(shard);
        return shards;
    }
 }
