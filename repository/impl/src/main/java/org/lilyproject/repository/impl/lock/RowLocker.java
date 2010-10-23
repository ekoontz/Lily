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
package org.lilyproject.repository.impl.lock;

import java.io.IOException;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class RowLocker {

    private final HTableInterface table;
    private final byte[] family;
    private final byte[] qualifier;
    private final long timeout;

    public RowLocker(HTableInterface table, byte[] family, byte[] qualifier, long timeout) {
        this.table = table;
        this.family = family;
        this.qualifier = qualifier;
        this.timeout = timeout;
    }
    
    public RowLock lockRow(byte[] rowKey) throws IOException {
        return lockRow(rowKey, null);
    }
    
    public RowLock lockRow(byte[] rowKey, org.apache.hadoop.hbase.client.RowLock hbaseRowLock) throws IOException {
        long now = System.currentTimeMillis();
        Get get = new Get(rowKey);
        get.addColumn(family, qualifier);
        org.apache.hadoop.hbase.client.RowLock actualHBaseRowLock;
        if (!table.exists(get)) {
            if (hbaseRowLock == null) {
                actualHBaseRowLock = table.lockRow(rowKey);
            } else {
                actualHBaseRowLock = hbaseRowLock;
            }
            if (actualHBaseRowLock == null) return null;
            try {
                Put put = new Put(rowKey, actualHBaseRowLock);
                byte[] lock = Bytes.toBytes(now);
                put.add(family, qualifier, lock);
                table.put(put);
                return new RowLock(rowKey, now);
            } finally {
                if (hbaseRowLock == null)
                try {
                    table.unlockRow(actualHBaseRowLock);
                } catch (IOException e) {
                    // ignore
                }
            }
        } else {
            Result result = table.get(get);
            byte[] previousLock = null;
            long previousTimestamp = -1L;
            if (!result.isEmpty()) {
                previousLock = result.getValue(family, qualifier);
                if (previousLock != null) {
                    previousTimestamp = Bytes.toLong(previousLock);
                }
            }
            if ((previousTimestamp == -1) || (previousTimestamp + timeout  < now)) {
                Put put = new Put(rowKey, hbaseRowLock);
                byte[] lock = Bytes.toBytes(now);
                put.add(family, qualifier, lock);
                if (table.checkAndPut(rowKey, family, qualifier, previousLock, put)) {
                    return new RowLock(rowKey, now);
                }
            }
            return null;
        }
    }
    
    public void unlockRow(RowLock lock) throws IOException {
        byte[] rowKey = lock.getRowKey();
        Put put = new Put(rowKey);
        put.add(family, qualifier, Bytes.toBytes(-1L));
        table.checkAndPut(rowKey, family, qualifier, Bytes.toBytes(lock.getTimestamp()), put); // If it fails, we already lost the lock
    }
    
    public boolean isLocked(byte[] rowKey) throws IOException {
        long now = System.currentTimeMillis();
        Get get = new Get(rowKey);
        get.addColumn(family, qualifier);
        Result result = table.get(get);

        if (result.isEmpty()) return false;
        
        byte[] lock = result.getValue(family, qualifier);
        if (lock == null) return false;
        
        long previousTimestamp = Bytes.toLong(lock);
        if (previousTimestamp == -1) return false;
        if (previousTimestamp + timeout < now) return false;
        
        return true;
    }
    
    public boolean put(Put put, RowLock lock) throws IOException {
        if (!Bytes.equals(put.getRow(), lock.getRowKey()))
                return false;
        return table.checkAndPut(lock.getRowKey(), family, qualifier, Bytes.toBytes(lock.getTimestamp()), put);
    }
}
