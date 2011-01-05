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
package org.lilyproject.rowlog.impl.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.util.hbase.LocalHTable;

public class RowLogTableUtil {
    
    private static final byte[] ROW_TABLE = Bytes.toBytes("rowTable");
    public static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("DATACF");
    public static final byte[] PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("PAYLOADCF");
    public static final byte[] EXECUTIONSTATE_COLUMN_FAMILY = Bytes.toBytes("ESLOGCF");

    public static HTableInterface getRowTable(Configuration configuration) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            admin.getTableDescriptor(ROW_TABLE);
        } catch (TableNotFoundException e) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(ROW_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(DATA_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(PAYLOAD_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(EXECUTIONSTATE_COLUMN_FAMILY));
            admin.createTable(tableDescriptor);
        }
        return new LocalHTable(configuration, ROW_TABLE);
    }
}
