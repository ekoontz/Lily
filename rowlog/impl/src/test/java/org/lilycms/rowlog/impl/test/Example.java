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
package org.lilycms.rowlog.impl.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;

public class Example {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    public static void main(String[] args) throws Exception {
        TEST_UTIL.startMiniCluster(1);
        Configuration configuration = TEST_UTIL.getConfiguration();
        String zkConnectString = configuration.get("hbase.zookeeper.quorum") + ":" + configuration.get("hbase.zookeeper.property.clientPort");
        // Create the row table
        final String ROW_TABLE = "rowTable";
        final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("DataCF");
        final byte[] PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("PAYLOADCF");
        final byte[] EXECUTIONSTATE_COLUMN_FAMILY = Bytes.toBytes("ESLOGCF");
        
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor tableDescriptor = new HTableDescriptor(ROW_TABLE);
        tableDescriptor.addFamily(new HColumnDescriptor(DATA_COLUMN_FAMILY));
        tableDescriptor.addFamily(new HColumnDescriptor(PAYLOAD_COLUMN_FAMILY));
        tableDescriptor.addFamily(new HColumnDescriptor(EXECUTIONSTATE_COLUMN_FAMILY));
        admin.createTable(tableDescriptor);
        HTable rowTable = new HTable(configuration, ROW_TABLE);
        
        // Create a RowLog instance
        RowLog rowLog = new RowLogImpl("Example", rowTable, PAYLOAD_COLUMN_FAMILY, EXECUTIONSTATE_COLUMN_FAMILY, 1000L, zkConnectString);
        
        // Create a shard and register it with the rowlog
        RowLogShard shard = new RowLogShardImpl("AShard", configuration, rowLog, 100);
        rowLog.registerShard(shard);
        
        // Create a consumer and register it with the RowLog
        RowLogMessageConsumer consumer = new FooBarConsumer();
        rowLog.registerConsumer(consumer);
        
        // The WAL use case 
        
        // Update a row with some user data
        // and put a message on the RowLog using the same put action
        byte[] row1 = Bytes.toBytes("row1");
        Put put = new Put(row1);
        put.add(DATA_COLUMN_FAMILY, Bytes.toBytes("AUserField"), Bytes.toBytes("SomeUserData"));
        RowLogMessage message = rowLog.putMessage(row1, Bytes.toBytes("SomeInfo"), Bytes.toBytes("Updated:AUserField"), put);
        rowTable.put(put);
        // Explicitly request the RowLog to process the message
        rowLog.processMessage(message);
        
        // The MQ use case
        
        // Create a processor and start it
        RowLogProcessor processor = new RowLogProcessorImpl(rowLog, shard, zkConnectString);
        processor.start();
        
        message  = rowLog.putMessage(row1, Bytes.toBytes("SomeMoreInfo"), Bytes.toBytes("Re-evaluate:AUserField"), null);
        
        // Give the processor some time to process the message
        Thread.sleep(30000);
        processor.stop();
        
        TEST_UTIL.shutdownMiniCluster();
    }
    
    private static class FooBarConsumer implements RowLogMessageConsumer {

        public static final int ID = 1;
        
        public FooBarConsumer() {
        }
        
        public int getId() {
            return ID;
        }
        
        public boolean processMessage(RowLogMessage message) {
                System.out.println("= Received a message =");
                System.out.println(Bytes.toString(message.getRowKey()));
                System.out.println(Bytes.toString(message.getData()));
            try {
                System.out.println(Bytes.toString(message.getPayload()));
            } catch (RowLogException e) {
                // ignore
            }
            return true;
        }
    }
    
}
