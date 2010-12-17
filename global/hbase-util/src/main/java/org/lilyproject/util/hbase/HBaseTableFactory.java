package org.lilyproject.util.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface HBaseTableFactory {

    HTableInterface getRecordTable() throws IOException;

    HTableInterface getTypeTable() throws IOException;

}
