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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.util.hbase.LocalHTable;
import org.lilyproject.util.io.Closer;

public class RowLogShardImpl implements RowLogShard {

    private static final byte[] PROBLEMATIC_MARKER = Bytes.toBytes("p");
    private static final byte[] MESSAGES_CF = Bytes.toBytes("messages");
    private static final byte[] MESSAGE_COLUMN = Bytes.toBytes("msg");
    private HTableInterface table;
    private final RowLog rowLog;
    private final String id;
    private final int batchSize;
    private Log log = LogFactory.getLog(getClass());

    public RowLogShardImpl(String id, Configuration configuration, RowLog rowLog, int batchSize) throws IOException {
        this.id = id;
        this.rowLog = rowLog;
        this.batchSize = batchSize;

        String tableName = rowLog.getId()+"-"+id;
        HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            admin.getTableDescriptor(Bytes.toBytes(tableName));
        } catch (TableNotFoundException e) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(MESSAGES_CF));
            admin.createTable(tableDescriptor);
        }

        table = new LocalHTable(configuration, tableName);
    }

    public String getId() {
        return id;
    }

    public void putMessage(RowLogMessage message) throws RowLogException {
        for (RowLogSubscription subscription : rowLog.getSubscriptions()) {
            putMessage(message, subscription.getId());
        }
    }

    public void putMessage(RowLogMessage message, List<RowLogSubscription> subscriptions) throws RowLogException {
        for (RowLogSubscription subscription : subscriptions) {
            putMessage(message, subscription.getId());
        }
    }

    private void putMessage(RowLogMessage message, String subscriptionId) throws RowLogException {
        byte[] rowKey = createRowKey(message, subscriptionId, false);
        Put put = new Put(rowKey);
        put.add(MESSAGES_CF, MESSAGE_COLUMN, encodeMessage(message));
        try {
            if (!table.checkAndPut(rowKey, MESSAGES_CF, MESSAGE_COLUMN, null, put)) {
                throw new RowLogException("Failed to put message on RowLogShard: concurrent update");
            }
        } catch (IOException e) {
            throw new RowLogException("Failed to put message on RowLogShard", e);
        }
    }

    public void removeMessage(RowLogMessage message, String subscription) throws RowLogException {
        removeMessage(message, subscription, false);
    }
    
    public void removeProblematicMessage(RowLogMessage message, String subscription) throws RowLogException {
        removeMessage(message, subscription, true);
    }

    private void removeMessage(RowLogMessage message, String subscription, boolean problematic) throws RowLogException {
        try {
            table.delete(new Delete(createRowKey(message, subscription, problematic)));
        } catch (IOException e) {
            throw new RowLogException("Failed to remove message from RowLogShard", e);
        }
    }

    public List<RowLogMessage> next(String subscription) throws RowLogException {
        return next(subscription, null, false);
    }

    public List<RowLogMessage> next(String subscription, Long minimalTimestamp) throws RowLogException {
        return next(subscription, minimalTimestamp, false);
    }
    
    public List<RowLogMessage> next(String subscription, Long minimalTimestamp, boolean problematic) throws RowLogException {
        byte[] rowPrefix;
        byte[] subscriptionBytes = Bytes.toBytes(subscription);
        if (problematic) {
            rowPrefix = PROBLEMATIC_MARKER;
            rowPrefix = Bytes.add(rowPrefix, subscriptionBytes);
        } else {
            rowPrefix = subscriptionBytes;
        }
        byte[] startRow = rowPrefix;
        if (minimalTimestamp != null) 
            startRow = Bytes.add(startRow, Bytes.toBytes(minimalTimestamp));
        try {
            List<RowLogMessage> rowLogMessages = new ArrayList<RowLogMessage>();
            Scan scan = new Scan(startRow);
            if (minimalTimestamp != null)
                scan.setTimeRange(minimalTimestamp, Long.MAX_VALUE);
            scan.addColumn(MESSAGES_CF, MESSAGE_COLUMN);
            ResultScanner scanner = table.getScanner(scan);
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
                        break; // There were no messages for this subscription
                    }
                    if (problematic) {
                        rowKey = Bytes.tail(rowKey, rowKey.length - PROBLEMATIC_MARKER.length);
                    }
                    byte[] value = next.getValue(MESSAGES_CF, MESSAGE_COLUMN);
                    byte[] messageId = Bytes.tail(rowKey, rowKey.length - subscriptionBytes.length);
                    rowLogMessages.add(decodeMessage(messageId, value));
                }
            } while(keepScanning);

            // The scanner is not closed in a finally block, since when we get an IOException from
            // HBase, it is likely that closing the scanner will give problems too. Not closing
            // the scanner is not fatal since HBase will expire it after a while.
            Closer.close(scanner);

            return rowLogMessages;
        } catch (IOException e) {
            throw new RowLogException("Failed to fetch next message from RowLogShard", e);
        }
    }

    public void markProblematic(RowLogMessage message, String subscription) throws RowLogException {
        byte[] rowKey = createRowKey(message, subscription, true);
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
        return next(subscription, null, true);
    }
    
    public boolean isProblematic(RowLogMessage message, String subscription) throws RowLogException {
        byte[] rowKey = createRowKey(message, subscription, true);
        try {
            return table.exists(new Get(rowKey));
        } catch (IOException e) {
            throw new RowLogException("Failed to check if message is problematic", e);
        }
    }

    private byte[] createRowKey(RowLogMessage message, String subscription, boolean problematic) {
        byte[] rowKey = new byte[0];
        if (problematic) {
            rowKey = PROBLEMATIC_MARKER;
        }
        rowKey = Bytes.add(rowKey, Bytes.toBytes(subscription));

        rowKey = Bytes.add(rowKey, Bytes.toBytes(message.getTimestamp()));
        rowKey = Bytes.add(rowKey, Bytes.toBytes(message.getSeqNr()));
        rowKey = Bytes.add(rowKey, message.getRowKey());
        
        return rowKey;
    }

    private byte[] encodeMessage(RowLogMessage message) {
        return message.getData();
    }

    private RowLogMessage decodeMessage(byte[] messageId, byte[] data) {
        long timestamp = Bytes.toLong(messageId);
        long seqNr = Bytes.toLong(messageId, Bytes.SIZEOF_LONG);
        byte[] rowKey = Bytes.tail(messageId, messageId.length - (2*Bytes.SIZEOF_LONG));
        return new RowLogMessageImpl(timestamp, rowKey, seqNr, data, rowLog);
    }

}
