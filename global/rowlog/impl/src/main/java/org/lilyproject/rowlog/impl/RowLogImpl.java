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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets.GetServlet;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.util.io.Closer;

/**
 * See {@link RowLog}
 */
public class RowLogImpl implements RowLog, SubscriptionsObserver, RowLogObserver {

    private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");
    private RowLogShard shard; // TODO: We only work with one shard for now
    private final HTableInterface rowTable;
    private final byte[] payloadColumnFamily;
    private final byte[] executionStateColumnFamily;
    private RowLogConfig rowLogConfig;
    
    private Map<String, RowLogSubscription> subscriptions = Collections.synchronizedMap(new HashMap<String, RowLogSubscription>());
    private final String id;
    private RowLogProcessorNotifier processorNotifier = null;
    private Log log = LogFactory.getLog(getClass());
    private RowLogConfigurationManager rowLogConfigurationManager;

    private final AtomicBoolean initialSubscriptionsLoaded = new AtomicBoolean(false);
    private final AtomicBoolean initialRowLogConfigLoaded = new AtomicBoolean(false);

    /**
     * The RowLog should be instantiated with information about the table that contains the rows the messages are 
     * related to, and the column families it can use within this table to put the payload and execution state of the
     * messages on.
     * @param rowTable the HBase table containing the rows to which the messages are related
     * @param payloadColumnFamily the column family in which the payload of the messages can be stored
     * @param executionStateColumnFamily the column family in which the execution state of the messages can be stored
     * @throws RowLogException
     */
    public RowLogImpl(String id, HTableInterface rowTable, byte[] payloadColumnFamily, byte[] executionStateColumnFamily,
            RowLogConfigurationManager rowLogConfigurationManager) throws InterruptedException {
        this.id = id;
        this.rowTable = rowTable;
        this.payloadColumnFamily = payloadColumnFamily;
        this.executionStateColumnFamily = executionStateColumnFamily;
        this.rowLogConfigurationManager = rowLogConfigurationManager;
        rowLogConfigurationManager.addRowLogObserver(id, this);
        synchronized (initialRowLogConfigLoaded) {
            while(!initialRowLogConfigLoaded.get()) {
                initialRowLogConfigLoaded.wait();
            }
        }
        this.processorNotifier = new RowLogProcessorNotifier(rowLogConfigurationManager, rowLogConfig.getNotifyDelay());
        rowLogConfigurationManager.addSubscriptionsObserver(id, this);
        synchronized (initialSubscriptionsLoaded) {
            while (!initialSubscriptionsLoaded.get()) {
                initialSubscriptionsLoaded.wait();
            }
        }
    }

    public void stop() {
        rowLogConfigurationManager.removeRowLogObserver(id, this);
        synchronized (initialRowLogConfigLoaded) {
            initialRowLogConfigLoaded.set(false);
        }
        rowLogConfigurationManager.removeSubscriptionsObserver(id, this);
        synchronized (initialSubscriptionsLoaded) {
            initialSubscriptionsLoaded.set(false);
        }
        Closer.close(processorNotifier);
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
    
    private long putPayload(byte[] rowKey, byte[] payload, long timestamp, Put put) throws IOException {
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
            put.add(payloadColumnFamily, rowLocalMessageQualifier(seqnr, timestamp), payload);
        } else {
            put = new Put(rowKey);
            put.add(payloadColumnFamily, SEQ_NR, Bytes.toBytes(seqnr));
            put.add(payloadColumnFamily, rowLocalMessageQualifier(seqnr, timestamp), payload);
            rowTable.put(put);
        }
        return seqnr;
    }

    private byte[] rowLocalMessageQualifier(long seqnr, long timestamp) {
        byte[] qualifier;
        qualifier = Bytes.toBytes(seqnr);
        qualifier = Bytes.add(qualifier, Bytes.toBytes(timestamp));
        return qualifier;
    }

    public byte[] getPayload(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(payloadColumnFamily, qualifier);
        Result result;
        try {
            result = rowTable.get(get);
        } catch (IOException e) {
            throw new RowLogException("Exception while getting payload from the rowTable", e);
        }
        return result.getValue(payloadColumnFamily, qualifier);
    }

    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws InterruptedException, RowLogException {
        RowLogShard shard = getShard(); // Fail fast if no shards are registered
        
        try {
            long now = System.currentTimeMillis();
            long seqnr = putPayload(rowKey, payload, now, put);
                    
            RowLogMessage message = new RowLogMessageImpl(now, rowKey, seqnr, data, this);

            // Take current snapshot of the subscriptions so that shard.putMessage and initializeSubscriptions
            // use the exact same set of subscriptions.
            List<RowLogSubscription> subscriptions = getSubscriptions();
            shard.putMessage(message, subscriptions);
            initializeSubscriptions(message, put, subscriptions);

            if (rowLogConfig.isEnableNotify()) {
                processorNotifier.notifyProcessor(id, shard.getId());
            }
            return message;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLog", e);
        }
    }

    
    private void initializeSubscriptions(RowLogMessage message, Put put, List<RowLogSubscription> subscriptions)
            throws IOException {
        SubscriptionExecutionState executionState = new SubscriptionExecutionState(message.getTimestamp());
        for (RowLogSubscription subscription : subscriptions) {
            executionState.setState(subscription.getId(), false);
        }
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
        if (put != null) {
            put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
        } else {
            put = new Put(message.getRowKey());
            put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
            rowTable.put(put);
        }
    }

    public boolean processMessage(RowLogMessage message) throws RowLogException, InterruptedException {
        byte[] rowKey = message.getRowKey();
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
            Get get = new Get(rowKey);
            get.addColumn(executionStateColumnFamily, qualifier);
            try {
                Result result = rowTable.get(get);
                if (result.isEmpty()) {
                    // No execution state was found indicating an orphan message on the global queue table
                    // Treat this message as if it was processed
                    return true;
                }
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

    private boolean processMessage(RowLogMessage message, SubscriptionExecutionState executionState) throws RowLogException, InterruptedException {
        boolean allDone = true;
        List<RowLogSubscription> subscriptionsSnapshot = getSubscriptions();
        if (rowLogConfig.isRespectOrder()) {
            Collections.sort(subscriptionsSnapshot);
        }
        for (RowLogSubscription subscription : getSubscriptions()) {
            String subscriptionId = subscription.getId();
            if (!executionState.getState(subscriptionId)) {
                boolean done = false;
                executionState.incTryCount(subscriptionId);
                RowLogMessageListener listener = RowLogMessageListenerMapping.INSTANCE.get(subscriptionId);
                if (listener != null) 
                    done = listener.processMessage(message);
                executionState.setState(subscriptionId, done);
                if (!done) {
                    allDone = false;
                    checkAndMarkProblematic(message, subscription, executionState);
                    if (rowLogConfig.isRespectOrder()) {
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
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
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
                if (previousTimestamp + rowLogConfig.getLockTimeout() < now) {
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
        RowLogSubscription subscription = subscriptions.get(subscriptionId);
        if (subscription == null)
            throw new RowLogException("Failed to unlock message, subscription " + subscriptionId + " no longer exists for rowlog " + this.getId());
        byte[] rowKey = message.getRowKey();
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
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
                checkAndMarkProblematic(message, subscription, executionState);
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
            if (rowLogConfig.isRespectOrder()) {
                List<RowLogSubscription> subscriptions = getSubscriptions();
                Collections.sort(subscriptions);
                for (RowLogSubscription otherSubscription : subscriptions) {
                    if (otherSubscription.getOrderNr() >= subscription.getOrderNr()) {
                        rowLogShard.markProblematic(message, otherSubscription.getId());
                        log.warn(String.format("Subscription %1$s failed to process message %2$s %3$s times. Respecting subscription order: subscription %4$s has been marked as problematic", 
                                subscription.getId(), message.toString(), maxTries, otherSubscription.getId()));
                    }
                }
            } else {
                rowLogShard.markProblematic(message, subscription.getId());
                log.warn(String.format("Subscription %1$s failed to process message %2$s %3$s times, it has been marked as problematic", 
                        subscription.getId(), message.toString(), maxTries));
            }
        }
    }
    
    public boolean isMessageLocked(RowLogMessage message, String subscriptionId) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        try {
            Result result = rowTable.get(get);
            if (result.isEmpty()) return false;
            
            SubscriptionExecutionState executionState = SubscriptionExecutionState.fromBytes(result.getValue(executionStateColumnFamily, qualifier));
            byte[] lock = executionState.getLock(subscriptionId);
            if (lock == null) return false;
        
            return (Bytes.toLong(lock) + rowLogConfig.getLockTimeout() > System.currentTimeMillis());
        } catch (IOException e) {
            throw new RowLogException("Failed to check if message is locked", e);
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
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
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
        if (executionState == null) {
            checkOrphanMessage(message, subscriptionId);
            return true;
        }
        return executionState.getState(subscriptionId);
    }

    private SubscriptionExecutionState getExecutionState(RowLogMessage message) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
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
        if (executionState == null) {
            checkOrphanMessage(message, subscriptionId);
            return false;
        }
        if (rowLogConfig.isRespectOrder()) {
            List<RowLogSubscription> subscriptions = getSubscriptions();
            Collections.sort(subscriptions);
            for (RowLogSubscription subscriptionContext : subscriptions) {
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
                        messages.add(new RowLogMessageImpl(executionState.getTimestamp(), rowKey, Bytes.toLong(entry.getKey()), null, this));
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
    
    /**
     * Checks if the message is orphaned, meaning there is a message on the global queue which has no representative on the row-local queue.
     * If the message is orphaned it is removed from the shard.
     * @param message the message to check
     * @param subscriptionId the subscription to check the message for
     */
    private void checkOrphanMessage(RowLogMessage message, String subscriptionId) throws RowLogException {
        Get get = new Get(message.getRowKey());
        byte[] qualifier = rowLocalMessageQualifier(message.getSeqNr(), message.getTimestamp());
        get.addColumn(executionStateColumnFamily, qualifier);
        Result result;
        try {
            result = rowTable.get(get);
            if (result.isEmpty()) {
                shard.removeMessage(message, subscriptionId);
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to check is message "+message+" is orphaned for subscription " + subscriptionId, e);
        }
    }
    
    public void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
        synchronized (subscriptions) {
            for (RowLogSubscription subscription : newSubscriptions) {
                subscriptions.put(subscription.getId(), subscription);
            }
            Iterator<RowLogSubscription> iterator = subscriptions.values().iterator();
            while (iterator.hasNext()) {
                RowLogSubscription subscription = iterator.next();
                if (!newSubscriptions.contains(subscription))
                    iterator.remove();
            }
        }
        if (!initialSubscriptionsLoaded.get()) {
            synchronized (initialSubscriptionsLoaded) {
                initialSubscriptionsLoaded.set(true);
                initialSubscriptionsLoaded.notifyAll();
            }
        }
    }

    public List<RowLogShard> getShards() {
        List<RowLogShard> shards = new ArrayList<RowLogShard>();
        shards.add(shard);
        return shards;
    }
    
    public void rowLogConfigChanged(RowLogConfig rowLogConfig) {
        this.rowLogConfig = rowLogConfig;
        if (!initialRowLogConfigLoaded.get()) {
            synchronized(initialRowLogConfigLoaded) {
                initialRowLogConfigLoaded.set(true);
                initialRowLogConfigLoaded.notifyAll();
            }
        }
    }

    
 }
