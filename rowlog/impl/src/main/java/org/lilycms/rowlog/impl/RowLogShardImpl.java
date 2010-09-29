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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.util.hbase.LocalHTable;
import org.lilycms.util.io.Closer;

public class RowLogShardImpl implements RowLogShard {

    private static final byte[] PROBLEMATIC_MARKER = Bytes.toBytes("P");
    private static final byte[] MESSAGES_CF = Bytes.toBytes("MESSAGES");
    private static final byte[] MESSAGE_COLUMN = Bytes.toBytes("MESSAGE");
    private HTableInterface table;
    private final RowLog rowLog;
    private final String id;
    private final int batchSize;

    public RowLogShardImpl(String id, Configuration configuration, RowLog rowLog, int batchSize) throws IOException {
        this.id = id;
        this.rowLog = rowLog;
        this.batchSize = batchSize;

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
        for (String subscriptionId: rowLog.getSubscriptionIds()) {
            putMessage(message, subscriptionId);
        }
    }

    private void putMessage(RowLogMessage message, String subscription) throws RowLogException {
        byte[] rowKey = createRowKey(message.getId(), subscription, false);
        Put put = new Put(rowKey);
        put.add(MESSAGES_CF, MESSAGE_COLUMN, encodeMessage(message));
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLogShard", e);
        }
    }

    public void removeMessage(RowLogMessage message, String subscription) throws RowLogException {
        // There is a slight chance that when marking a message as problematic,
        // the problematic message was put on the table, but the non-problematic
        // message was not removed. So we always try to remove both.
        removeMessage(message, subscription, false);
        removeMessage(message, subscription, true);
    }

    private void removeMessage(RowLogMessage message, String subscription, boolean problematic) throws RowLogException {
        byte[] rowKey = createRowKey(message.getId(), subscription, problematic);

        RowLock rowLock = null;
        try {
            if (table.exists(new Get(rowKey))) {
                rowLock = table.lockRow(rowKey);
                Delete delete = new Delete(rowKey, Long.MAX_VALUE, rowLock);
                table.delete(delete);
            }
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

    public List<RowLogMessage> next(String subscription) throws RowLogException {
        return next(subscription, false);
    }
    
    public List<RowLogMessage> next(String subscription, boolean problematic) throws RowLogException {
        byte[] rowPrefix = null;
        byte[] subscriptionBytes = Bytes.toBytes(subscription);
        if (problematic) {
            rowPrefix = PROBLEMATIC_MARKER;
            rowPrefix = Bytes.add(rowPrefix, subscriptionBytes);
        } else {
            rowPrefix = subscriptionBytes;
        }
        Scan scan = new Scan(rowPrefix);
        scan.addColumn(MESSAGES_CF, MESSAGE_COLUMN);
        ResultScanner scanner = null;
        List<RowLogMessage> rowLogMessages = new ArrayList<RowLogMessage>();
        try {
            scanner = table.getScanner(scan);
            boolean keepScanning = problematic;
            do {
                Result[] results = scanner.next(batchSize);
                if (results.length == 0) {
                    keepScanning = false;
                }
                for (Result next : results) {
                    byte[] rowKey = next.getRow();
                    if (!Bytes.startsWith(rowKey, rowPrefix)) {
                        keepScanning = false;
                        break; // There were no messages for this consumer
                    }
                    if (problematic) {
                        rowKey = Bytes.tail(rowKey, rowKey.length - PROBLEMATIC_MARKER.length);
                    }
                    byte[] value = next.getValue(MESSAGES_CF, MESSAGE_COLUMN);
                    byte[] messageId = Bytes.tail(rowKey, rowKey.length - subscriptionBytes.length);
                    rowLogMessages.add(decodeMessage(messageId, value));
                }
            } while(keepScanning);
            return rowLogMessages;
        } catch (IOException e) {
            throw new RowLogException("Failed to fetch next message from RowLogShard", e);
        } finally {
            Closer.close(scanner);
        }
    }

    public void markProblematic(RowLogMessage message, String subscription) throws RowLogException {
        byte[] rowKey = createRowKey(message.getId(), subscription, true);
        Put put = new Put(rowKey);
        put.add(MESSAGES_CF, MESSAGE_COLUMN, encodeMessage(message));
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RowLogException("Failed to mark message as problematic", e);
        }
        removeMessage(message, subscription, false);
    }

    public List<RowLogMessage> getProblematic(String subscription) throws RowLogException {
        return next(subscription, true);
    }
    
    public boolean isProblematic(RowLogMessage message, String subscription) throws RowLogException {
        byte[] rowKey = createRowKey(message.getId(), subscription, true);
        try {
            return table.exists(new Get(rowKey));
        } catch (IOException e) {
            throw new RowLogException("Failed to check if message is problematic", e);
        }
    }

    private byte[] createRowKey(byte[] messageId, String subscription, boolean problematic) {
        byte[] rowKey = new byte[0];
        if (problematic) {
            rowKey = PROBLEMATIC_MARKER;
        }
        rowKey = Bytes.add(rowKey, Bytes.toBytes(subscription));
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
        int rowKeyLength = Bytes.toInt(bytes, Bytes.SIZEOF_LONG);
        byte[] rowKey = Bytes
                .head(Bytes.tail(bytes, bytes.length - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT), rowKeyLength);
        int dataLength = bytes.length - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT - rowKeyLength;
        byte[] data = null;
        if (dataLength > 0) {
            data = Bytes.tail(bytes, dataLength);
        }
        return new RowLogMessageImpl(messageId, rowKey, seqnr, data, rowLog);
    }

}
