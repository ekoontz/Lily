package org.lilycms.repository.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.util.hbase.LocalHTable;

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
