package org.lilycms.testfw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Provides access to HBase, either by starting an embedded HBase or by connecting to a running HBase.
 *
 * <p>This is intended for usage in test cases.
 *
 * <p><b>VERY VERY IMPORTANT</b>: when connecting to an existing HBase, this class will DELETE ALL ROWS
 * FROM ALL TABLES!
 */
public class HBaseProxy {
    private static Mode MODE;
    private static Configuration CONF;
    private static HBaseTestingUtility TEST_UTIL;

    private enum Mode { EMBED, CONNECT }
    private static String HBASE_MODE_PROP_NAME = "lily.test.hbase";

    private static Set<String> RETAIN_TABLES = new HashSet<String>();
    static {
        RETAIN_TABLES.add("indexmeta");
    }

    // For some tables, we exploit the timestamp dimension by using custom timestamps, which
    // for certain tests (where the same row key and timestamp is reused) cause problems, due
    // to the use of non-increasing timestamps, see also http://markmail.org/message/xskvbzhrvkv7skxz
    // or http://search-hadoop.com/m/rNnhN15Xecu
    // For these tables, we need to flush and compact them, and wait for this to complete,
    // before continuing.
    // This map contains as key the name of the table and as value the name of the/a column family where versions
    // are used in this way.
    private static Map<String, byte[]> EXPLOIT_TIMESTAMP_TABLES = new HashMap<String, byte[]>();
    static {
        EXPLOIT_TIMESTAMP_TABLES.put("recordTable", Bytes.toBytes("VCF"));
        EXPLOIT_TIMESTAMP_TABLES.put("typeTable", Bytes.toBytes("FTECF"));
    }

    public void start() throws Exception {
        String hbaseModeProp = System.getProperty(HBASE_MODE_PROP_NAME);
        if (hbaseModeProp == null || hbaseModeProp.equals("") || hbaseModeProp.equals("embed")) {
            MODE = Mode.EMBED;
        } else if (hbaseModeProp.equals("connect")) {
            MODE = Mode.CONNECT;
        } else {
            throw new RuntimeException("Unexpected value for " + HBASE_MODE_PROP_NAME + ": " + hbaseModeProp);
        }

        System.out.println("HBase usage mode: " + MODE);

        CONF = HBaseConfiguration.create();

        switch (MODE) {
            case EMBED:
                addHBaseTestProps(CONF);
                addUserProps(CONF);
                TEST_UTIL = new HBaseTestingUtility(CONF);
                TEST_UTIL.startMiniCluster(1);
                CONF = TEST_UTIL.getConfiguration();
                break;
            case CONNECT:
                CONF.set("hbase.zookeeper.quorum", "localhost");
                CONF.set("hbase.zookeeper.property.clientPort", "2181"); // matches HBaseRunner
                addUserProps(CONF);
                cleanTables();
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + MODE);
        }
    }

    public String getZkConnectString() {
        return CONF.get("hbase.zookeeper.quorum") + ":" + CONF.get("hbase.zookeeper.property.clientPort");
    }
    
    /**
     * Adds all system property prefixed with "lily.test.hbase." to the HBase configuration.
     */
    private void addUserProps(Configuration conf) {
        Properties sysProps = System.getProperties();
        for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
            String name = entry.getKey().toString();
            if (name.startsWith("lily.test.hbase.")) {
                String hbasePropName = name.substring("lily.test.".length());
                conf.set(hbasePropName, entry.getValue().toString());
            }
        }
    }

    protected static void addHBaseTestProps(Configuration conf) {
        // The following properties are from HBase's src/test/resources/hbase-site.xml
        conf.set("hbase.regionserver.msginterval", "1000");
        conf.set("hbase.client.pause", "5000");
        conf.set("hbase.client.retries.number", "4");
        conf.set("hbase.master.meta.thread.rescanfrequency", "10000");
        conf.set("hbase.server.thread.wakefrequency", "1000");
        conf.set("hbase.regionserver.handler.count", "5");
        conf.set("hbase.master.info.port", "-1");
        conf.set("hbase.regionserver.info.port", "-1");
        conf.set("hbase.regionserver.info.port.auto", "true");
        conf.set("hbase.master.lease.thread.wakefrequency", "3000");
        conf.set("hbase.regionserver.optionalcacheflushinterval", "1000");
        conf.set("hbase.regionserver.safemode", "false");
    }

    public void stop() throws Exception {
        if (MODE == Mode.EMBED) {
            TEST_UTIL.shutdownMiniCluster();
            TEST_UTIL = null;
        }
        CONF = null;
    }

    public Configuration getConf() {
        return CONF;
    }

    public FileSystem getBlobFS() throws IOException, URISyntaxException {
        if (MODE == Mode.EMBED) {
            return TEST_UTIL.getDFSCluster().getFileSystem();
        } else {
            String dfsUri = System.getProperty("lily.test.dfs");

            if (dfsUri == null) {
                dfsUri = "hdfs://localhost:9000";
            }

            return FileSystem.get(new URI(dfsUri), getConf());
        }
    }

    private void cleanTables() throws Exception {
        System.out.println("------------------------ Resetting HBase tables ------------------------");
        
        StringBuilder truncateReport = new StringBuilder();
        StringBuilder retainReport = new StringBuilder();

        HBaseAdmin admin = new HBaseAdmin(getConf());
        HTableDescriptor[] tables = admin.listTables();
        System.out.println("Found tables: " + tables.length);

        Set<String> exploitTimestampTables = new HashSet<String>();

        for (HTableDescriptor table : tables) {
            if (RETAIN_TABLES.contains(table.getNameAsString())) {
                if (retainReport.length() > 0)
                    retainReport.append(", ");
                retainReport.append(table.getNameAsString());
                continue;
            }

            HTable htable = new HTable(getConf(), table.getName());

            if (EXPLOIT_TIMESTAMP_TABLES.containsKey(table.getNameAsString())) {
                insertTimestampTableTestRecord(table.getNameAsString(), htable);
                exploitTimestampTables.add(table.getNameAsString());
            }

            Scan scan = new Scan();
            ResultScanner scanner = htable.getScanner(scan);
            Result[] results;
            int totalCount = 0;

            while ((results = scanner.next(1000)).length > 0) {
                List<Delete> deletes = new ArrayList<Delete>(results.length);
                for (Result result : results) {
                    deletes.add(new Delete(result.getRow()));
                }
                totalCount += deletes.size();
                htable.delete(deletes);
            }

            if (truncateReport.length() > 0)
                truncateReport.append(", ");
            truncateReport.append(table.getNameAsString()).append(" (").append(totalCount).append(")");

            scanner.close();
            htable.close();

            if (EXPLOIT_TIMESTAMP_TABLES.containsKey(table.getNameAsString())) {
                admin.flush(table.getName());
                admin.majorCompact(table.getName());
            }
        }

        truncateReport.insert(0, "Truncated the following tables: ");
        retainReport.insert(0, "Did not truncate the following tables: ");

        System.out.println(truncateReport);
        System.out.println(retainReport);

        waitForTimestampTables(exploitTimestampTables);

        System.out.println("------------------------------------------------------------------------");

    }

    private void insertTimestampTableTestRecord(String tableName, HTable htable) throws IOException {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] CF = EXPLOIT_TIMESTAMP_TABLES.get(tableName);
        byte[] COL = Bytes.toBytes("DummyColumn");
        Put put = new Put(tmpRowKey);
        // put a value with a fixed timestamp
        put.add(CF, COL, 1, new byte[] { 0 });

        htable.put(put);
    }

    private void waitForTimestampTables(Set<String> tables) throws IOException, InterruptedException {
        for (String tableName : tables) {

            HTable htable = new HTable(CONF, tableName);

            byte[] CF = EXPLOIT_TIMESTAMP_TABLES.get(tableName);
            byte[] tmpRowKey = waitForCompact(tableName, CF);

            // Delete our dummy row again
            htable.delete(new Delete(tmpRowKey));
        }
    }

    private byte[] waitForCompact(String tableName, byte[] CF) throws IOException, InterruptedException {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HTable htable = new HTable(CONF, tableName);

        byte[] value = null;
        while (value == null) {
            Put put = new Put(tmpRowKey);
            put.add(CF, COL, 1, new byte[] { 0 });
            htable.put(put);

            Get get = new Get(tmpRowKey);
            Result result = htable.get(get);
            value = result.getValue(CF, COL);
            if (value == null) {
                // If the value is null, it is because the delete marker has not yet been flushed/compacted away
                System.out.println("Waiting for flush/compact of " + tableName + " to complete");
                Thread.sleep(100);
            }
        }
        return tmpRowKey;
    }

    /** Force a major compaction and wait for it to finish.
     *  This method can be used in a test to avoid issue HBASE-2256 after performing a delete operation 
     *  Uses same principle as {@link #cleanTables()}
     */ 
    public void majorCompact(String tableName, String[] columnFamilies) throws Exception {
        byte[] tmpRowKey = Bytes.toBytes("HBaseProxyDummyRow");
        byte[] COL = Bytes.toBytes("DummyColumn");
        HBaseAdmin admin = new HBaseAdmin(getConf());
        HTable htable = new HTable(CONF, tableName);
        
        // Write a dummy row
        for (String columnFamily : columnFamilies) {
            byte[] CF = Bytes.toBytes(columnFamily);
            Put put = new Put(tmpRowKey);
            put.add(CF, COL, 1, new byte[] { 0 });
            htable.put(put);
            // Delete the value again
            Delete delete = new Delete(tmpRowKey);
            delete.deleteColumn(CF, COL);
            htable.delete(delete);
        }
        
        // Perform major compaction
        admin.flush(tableName);
        admin.majorCompact(tableName);
        
        // Wait for compact to finish
        for (String columnFamily : columnFamilies) {
            byte[] CF = Bytes.toBytes(columnFamily);
            waitForCompact(tableName, CF);
        }
    }
}
