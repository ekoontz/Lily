package org.lilycms.util.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * This is a threadsafe solution for the non-threadsafe HTable.
 *
 * <p>The problem with HTable this tries to solve is that HTable is not threadsafe, and
 * on the other hand it is best not to instantiate a new HTable for each use for
 * performance reasons.
 *
 * <p>HTable is, unlike e.g. a file handle or a JDBC connection, not a scarce
 * resource which needs to be closed. The actual connection handling (which
 * consists of connections to a variety of region servers) it handled at other
 * places. We only need to avoid the cost of creating new copies of HTable all the time.
 *
 * <p>Therefore, an ideal solution is to cache HTable instances in threadlocal variables.
 *
 * <p>This solution is fine for the following situations:
 *
 * <ul>
 *   <li>Multiple threads access the same instance of LocalHTable, e.g. because it
 *       is used in a singleton service class or declared as a static variable.
 *   <li>The threads which make use of the HTable are long-running or pooled threads.
 * </ul>
 *
 * <p>Be careful/considerate when using autoflush.
 *
 * <p>The current implementation will still cause multiple HTable's to be instantiated
 * for the same {conf, table} pair on the same thread if you use multiple LocalHTable's.
 *
 * <p>An alternative solution is the HTablePool provided by HBase.
 */
public class LocalHTable extends ThreadLocal<HTable> implements HTableInterface {
    private Configuration conf;
    private byte[] tableName;

    public LocalHTable(Configuration conf, byte[] tableName) {
        this.conf = conf;
        this.tableName = tableName;
    }

    public LocalHTable(Configuration conf, String tableName) {
        this.conf = conf;
        this.tableName = Bytes.toBytes(tableName);
    }

    @Override
    protected HTable initialValue() {
        try {
            return new HTable(conf, tableName);
        } catch (IOException e) {
            throw new RuntimeException("Error getting HTable.", e);
        }
    }

    public byte[] getTableName() {
        return get().getTableName();
    }

    public Configuration getConfiguration() {
        return get().getConfiguration();
    }

    public HTableDescriptor getTableDescriptor() throws IOException {
        return get().getTableDescriptor();
    }

    public boolean exists(Get get) throws IOException {
        return get().exists(get);
    }

    public Result get(Get get) throws IOException {
        return get().get(get);
    }

    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        return get().getRowOrBefore(row, family);
    }

    public ResultScanner getScanner(Scan scan) throws IOException {
        return get().getScanner(scan);
    }

    public ResultScanner getScanner(byte[] family) throws IOException {
        return get().getScanner(family);
    }

    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        return get().getScanner(family, qualifier);
    }

    public void put(Put put) throws IOException {
        get().put(put);
    }

    public void put(List<Put> puts) throws IOException {
        get().put(puts);
    }

    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        return get().checkAndPut(row, family, qualifier, value, put);
    }

    public void delete(Delete delete) throws IOException {
        get().delete(delete);
    }

    public void delete(List<Delete> deletes) throws IOException {
        get().delete(deletes);
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        return get().checkAndDelete(row, family, qualifier, value, delete);
    }

    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return get().incrementColumnValue(row, family, qualifier, amount);
    }

    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
        return get().incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    }

    public boolean isAutoFlush() {
        return get().isAutoFlush();
    }

    public void flushCommits() throws IOException {
        get().flushCommits();
    }

    public void close() throws IOException {
        get().close();
    }

    public RowLock lockRow(byte[] row) throws IOException {
        return get().lockRow(row);
    }

    public void unlockRow(RowLock rl) throws IOException {
        get().unlockRow(rl);
    }
}
