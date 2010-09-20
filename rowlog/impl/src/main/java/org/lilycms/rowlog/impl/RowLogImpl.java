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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogShard;

/**
 * See {@link RowLog}
 */
public class RowLogImpl implements RowLog {

    private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");
    private RowLogShard shard;
    private final HTableInterface rowTable;
    private final byte[] payloadColumnFamily;
    private final byte[] executionStateColumnFamily;
    
    private Map<Integer, RowLogMessageConsumer> consumers = new TreeMap<Integer, RowLogMessageConsumer>();
    private final long lockTimeout;
    private final String id;
    private RowLogProcessorNotifier processorNotifier = null;
    private Log log = LogFactory.getLog(getClass());
    
    /**
     * The RowLog should be instantiated with information about the table that contains the rows the messages are 
     * related to, and the column families it can use within this table to put the payload and execution state of the
     * messages on.
     * @param rowTable the HBase table containing the rows to which the messages are related
     * @param payloadColumnFamily the column family in which the payload of the messages can be stored
     * @param executionStateColumnFamily the column family in which the execution state of the messages can be stored
     * @param lockTimeout the timeout to be used for the locks that are put on the messages
     */
    public RowLogImpl(String id, HTableInterface rowTable, byte[] payloadColumnFamily, byte[] executionStateColumnFamily, long lockTimeout, String zkConnectString) {
        this.id = id;
        this.rowTable = rowTable;
        this.payloadColumnFamily = payloadColumnFamily;
        this.executionStateColumnFamily = executionStateColumnFamily;
        this.lockTimeout = lockTimeout;
        if (zkConnectString != null) {
            try {
                this.processorNotifier = new RowLogProcessorNotifier(zkConnectString);
            } catch (RowLogException exception) {
                // Don't use a processorNotifier
            }
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        if (processorNotifier != null) {
            processorNotifier.close();
        }
        super.finalize();
    }
    
    public String getId() {
        return id;
    }
    
    public void registerConsumer(RowLogMessageConsumer rowLogMessageConsumer) {
        consumers.put(rowLogMessageConsumer.getId(), rowLogMessageConsumer);
    }
    
    public void unRegisterConsumer(RowLogMessageConsumer rowLogMessageConsumer) {
        consumers.remove(rowLogMessageConsumer.getId());
    }
    
    public Collection<RowLogMessageConsumer> getConsumers() {
        return consumers.values();
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
        
            shard.putMessage(message);
            initializeConsumers(message, put);
            if (processorNotifier != null) {
                processorNotifier.notifyProcessor(id, shard.getId());
            }
            return message;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLog", e);
        }
    }

    
    private void initializeConsumers(RowLogMessage message, Put put) throws IOException {
        RowLogMessageConsumerExecutionState executionState = new RowLogMessageConsumerExecutionState(message.getId());
        for (Integer consumerId : consumers.keySet()) {
            executionState.setState(consumerId, false);
            executionState.incTryCount(consumerId);
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
                RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);

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

    private boolean processMessage(RowLogMessage message, RowLogMessageConsumerExecutionState executionState) throws RowLogException {
        boolean allDone = true;
        for (Entry<Integer, RowLogMessageConsumer> entry : consumers.entrySet()) {
            int consumerId = entry.getKey();
            if (!executionState.getState(consumerId)) {
                boolean done = false;
                try {
                    executionState.incTryCount(consumerId);
                    done = entry.getValue().processMessage(message);
                } catch (Throwable t) {
                    executionState.setState(consumerId, false);
                    return false;
                }
                executionState.setState(consumerId, done);
                if (!done) {
                    allDone = false;
                    checkAndMarkProblematic(message, consumerId, executionState);
                } else {
                    shard.removeMessage(message, consumerId);
                }
            }
        }
        return allDone;
    }
    
    public byte[] lockMessage(RowLogMessage message, int consumerId) throws RowLogException {
        return lockMessage(message, consumerId, 0);
    }
    
    private byte[] lockMessage(RowLogMessage message, int consumerId, int count) throws RowLogException {
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
            RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
            byte[] previousLock = executionState.getLock(consumerId);
            long now = System.currentTimeMillis();
            if (previousLock == null) {
                return putLock(message, consumerId, rowKey, qualifier, previousValue, executionState, now, count);
            } else {
                long previousTimestamp = Bytes.toLong(previousLock);
                if (previousTimestamp + lockTimeout < now) {
                    return putLock(message, consumerId, rowKey, qualifier, previousValue, executionState, now, count);
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to lock message", e);
        }
    }

    private byte[] putLock(RowLogMessage message, int consumerId, byte[] rowKey, byte[] qualifier, byte[] previousValue,
            RowLogMessageConsumerExecutionState executionState, long now, int count) throws RowLogException {
        byte[] lock = Bytes.toBytes(now);
        executionState.setLock(consumerId, lock);
        executionState.incTryCount(consumerId);
        Put put = new Put(rowKey);
        put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
        try {
            if (!rowTable.checkAndPut(rowKey, executionStateColumnFamily, qualifier, previousValue, put)) {
                return lockMessage(message, consumerId, count+1); // Retry
            } else {
                return lock;
            }
        } catch (IOException e) {
            return lockMessage(message, consumerId, count+1); // Retry
        }
    }
    
    public boolean unlockMessage(RowLogMessage message, int consumerId, byte[] lock) throws RowLogException {
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
            RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
            byte[] previousLock = executionState.getLock(consumerId);
            if (!Bytes.equals(lock, previousLock)) return false; // The lock was lost  
            
            executionState.setLock(consumerId, null);
            Put put = new Put(rowKey);
            put.add(executionStateColumnFamily, qualifier, executionState.toBytes());
            boolean unLocked = rowTable.checkAndPut(rowKey, executionStateColumnFamily, qualifier, previousValue, put);
            if (unLocked) {
                checkAndMarkProblematic(message, consumerId, executionState);
            }
            return unLocked; 
        } catch (IOException e) {
            throw new RowLogException("Failed to unlock message", e);
        }
    }

    private void checkAndMarkProblematic(RowLogMessage message, int consumerId,
            RowLogMessageConsumerExecutionState executionState) throws RowLogException {
        int maxTries = consumers.get(consumerId).getMaxTries();
        if (executionState.getTryCount(consumerId) >= maxTries) {
            getShard().markProblematic(message, consumerId);
            log.warn(String.format("Consumer %1$s failed to process message %2$s %3$s times, and it has been marked as problematic", consumerId, message.getId(), maxTries));
        }
    }
    
    public boolean isMessageLocked(RowLogMessage message, int consumerId) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        try {
            Result result = rowTable.get(get);
            if (result.isEmpty()) return false;
            
            RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(result.getValue(executionStateColumnFamily, qualifier));
            byte[] lock = executionState.getLock(consumerId);
            if (lock == null) return false;
        
            return (Bytes.toLong(lock) + lockTimeout > System.currentTimeMillis());
        } catch (IOException e) {
            throw new RowLogException("Failed to check if messages is locked", e);
        }
    }
    
    public boolean messageDone(RowLogMessage message, int consumerId, byte[] lock) throws RowLogException {
        return messageDone(message, consumerId, lock, 0);
    }
    
    private boolean messageDone(RowLogMessage message, int consumerId, byte[] lock, int count) throws RowLogException {
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
                RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
                if (!Bytes.equals(lock,executionState.getLock(consumerId))) {
                    return false; // Not owning the lock
                }
                executionState.setState(consumerId, true);
                executionState.incTryCount(consumerId);
                executionState.setLock(consumerId, null);
                if (executionState.allDone()) {
                    removeExecutionStateAndPayload(rowKey, qualifier, previousValue);
                } else {
                    if (!updateExecutionState(rowKey, qualifier, executionState, previousValue)) {
                        return messageDone(message, consumerId, lock, count+1); // Retry
                    }
                }
            }
            shard.removeMessage(message, consumerId);
            return true;
        } catch (IOException e) {
            throw new RowLogException("Failed to put message to done", e);
        }
    }
    
    public boolean isMessageDone(RowLogMessage message, int consumerId) throws RowLogException {
        byte[] rowKey = message.getRowKey();
        long seqnr = message.getSeqNr();
        byte[] qualifier = Bytes.toBytes(seqnr);
        Get get = new Get(rowKey);
        get.addColumn(executionStateColumnFamily, qualifier);
        try {
            Result result = rowTable.get(get);
            if (result.isEmpty())
                return true;
            byte[] previousValue = result.getValue(executionStateColumnFamily, qualifier);
            RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(previousValue);
            return executionState.getState(consumerId);
        } catch (IOException e) {
            throw new RowLogException("Failed to check if message is done", e);
        }
    }

    private boolean updateExecutionState(byte[] rowKey, byte[] qualifier, RowLogMessageConsumerExecutionState executionState, byte[] previousValue) throws IOException {
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

    public List<RowLogMessage> getMessages(byte[] rowKey, int ... consumerId) throws RowLogException {
        List<RowLogMessage> messages = new ArrayList<RowLogMessage>();
        Get get = new Get(rowKey);
        get.addFamily(executionStateColumnFamily);
        try {
            Result result = rowTable.get(get);
            if (!result.isEmpty()) {
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(executionStateColumnFamily);
                for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    RowLogMessageConsumerExecutionState executionState = RowLogMessageConsumerExecutionState.fromBytes(entry.getValue());
                    boolean add = false;
                    if (consumerId.length == 0)
                        add = true;
                    else {
                        for (int cid : consumerId) {
                            if (!executionState.getState(cid))
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
    
    public List<RowLogMessage> getProblematic(int consumerId) throws RowLogException {
        return getShard().getProblematic(consumerId);
    }
    
    public boolean isProblematic(RowLogMessage message, int consumerId) throws RowLogException {
        return getShard().isProblematic(message, consumerId);
    }
    
    public RowLogMessageConsumer getConsumer(int consumerId) {
        return consumers.get(consumerId);
    }
 }
