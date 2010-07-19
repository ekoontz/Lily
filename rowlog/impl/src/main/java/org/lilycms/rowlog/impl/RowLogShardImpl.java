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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.util.hbase.LocalHTable;
import org.lilycms.util.io.Closer;

public class RowLogShardImpl implements RowLogShard {

    private static final byte[] MESSAGES_CF = Bytes.toBytes("MESSAGES");
    private static final byte[] MESSAGE_COLUMN = Bytes.toBytes("MESSAGE");
    private HTableInterface table;
    private final RowLog rowLog;
    private final String id;

    public RowLogShardImpl(String id, Configuration configuration, RowLog rowLog) throws IOException {
        this.id = id;
        this.rowLog = rowLog;

        HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            admin.getTableDescriptor(Bytes.toBytes(id));
        } catch (TableNotFoundException e) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(id);
            tableDescriptor.addFamily(new HColumnDescriptor(MESSAGES_CF));
            admin.createTable(tableDescriptor);
        }

        table = new LocalHTable(configuration, id);
    }
    
    public String getId() {
        return id;
    }
    
    public void putMessage(RowLogMessage message) throws RowLogException {
        for (RowLogMessageConsumer consumer : rowLog.getConsumers()) {
            putMessage(message, consumer.getId());
        }
    }
    
    private void putMessage(RowLogMessage message, int consumerId) throws RowLogException {
        byte[] rowKey = createRowKey(message.getId(), consumerId);
        Put put = new Put(rowKey);
        put.add(MESSAGES_CF, MESSAGE_COLUMN, encodeMessage(message));
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLogShard", e);
        }
    }
    
    public void removeMessage(RowLogMessage message, int consumerId) throws RowLogException {
        byte[] rowKey = createRowKey(message.getId(), consumerId);
        
        RowLock rowLock = null;
        try {
            rowLock = table.lockRow(rowKey);
            Delete delete = new Delete(rowKey, Long.MAX_VALUE, rowLock);
            table.delete(delete);
        } catch (IOException e) {
            throw new RowLogException("Failed to remove message from RowLogShard", e);
        } finally {
            if (rowLock != null) {
                try {
                    table.unlockRow(rowLock);
                } catch (IOException e) {
                    // Ignore, the lock will timeout eventually
                }
            }
        }
    }

    public RowLogMessage next(int consumerId) throws RowLogException {
        Scan scan = new Scan(Bytes.toBytes(consumerId));
        scan.addColumn(MESSAGES_CF, MESSAGE_COLUMN);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            Result next = scanner.next();
            if (next == null)
                return null;
            byte[] rowKey = next.getRow();
            int actualConsumerId = Bytes.toInt(rowKey);
            if (consumerId != actualConsumerId)
                return null; // There were no messages for this consumer
            byte[] value = next.getValue(MESSAGES_CF, MESSAGE_COLUMN);
            byte[] messageId = Bytes.tail(rowKey, rowKey.length - Bytes.SIZEOF_INT);
            return decodeMessage(messageId, value);
        } catch (IOException e) {
            throw new RowLogException("Failed to fetch next message from RowLogShard", e);
        } finally {
            Closer.close(scanner);
        }
    }

    private byte[] createRowKey(byte[] messageId, int consumerId) {
        byte[] rowKey = Bytes.toBytes(consumerId);
        rowKey = Bytes.add(rowKey, messageId);
        return rowKey;
    }
    
    private byte[] encodeMessage(RowLogMessage message) {
        byte[] bytes = Bytes.toBytes(message.getSeqNr());
        bytes = Bytes.add(bytes, Bytes.toBytes(message.getRowKey().length));
        bytes = Bytes.add(bytes, message.getRowKey());
        if (message.getData() != null) {
            bytes = Bytes.add(bytes, message.getData());
        }
        return bytes;
    }
    
    private RowLogMessage decodeMessage(byte[] messageId, byte[] bytes) {
        long seqnr = Bytes.toLong(bytes);
        int rowKeyLength = Bytes.toInt(bytes,Bytes.SIZEOF_LONG);
        byte[] rowKey = Bytes.head(Bytes.tail(bytes, bytes.length-Bytes.SIZEOF_LONG-Bytes.SIZEOF_INT), rowKeyLength);
        int dataLength = bytes.length - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT - rowKeyLength;
        byte[] data = null;
        if (dataLength > 0) {
            data = Bytes.tail(bytes, dataLength);
        }
        return new RowLogMessageImpl(messageId, rowKey, seqnr, data, rowLog);
    }

}
