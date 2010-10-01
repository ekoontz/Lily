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
package org.lilycms.util.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableUtil {
    
    private static final byte[] RECORD_TABLE = Bytes.toBytes("recordTable");
    
    public static final byte[] NON_VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("NVSCF");
    public static final byte[] VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("VSCF");
    public static final byte[] NON_VERSIONED_COLUMN_FAMILY = Bytes.toBytes("NVCF");
    public static final byte[] VERSIONED_COLUMN_FAMILY = Bytes.toBytes("VCF");
    public static final byte[] VERSIONED_MUTABLE_COLUMN_FAMILY = Bytes.toBytes("VMCF");

    public static final byte[] WAL_PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("WPLCF");
    public static final byte[] WAL_COLUMN_FAMILY = Bytes.toBytes("WALCF");
    public static final byte[] MQ_PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("MQPLCF");
    public static final byte[] MQ_COLUMN_FAMILY = Bytes.toBytes("MQCF");

    public static HTableInterface getRecordTable(Configuration configuration) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            admin.getTableDescriptor(RECORD_TABLE);
        } catch (TableNotFoundException e) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(RECORD_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_SYSTEM_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_SYSTEM_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
            tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_MUTABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
            tableDescriptor.addFamily(new HColumnDescriptor(WAL_PAYLOAD_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(WAL_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(MQ_PAYLOAD_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(MQ_COLUMN_FAMILY));
            admin.createTable(tableDescriptor);
        }

        return new LocalHTable(configuration, RECORD_TABLE);
    }
}
