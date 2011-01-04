package org.lilyproject.clientmetrics;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

public class HBaseMetrics {
    private HBaseAdmin hbaseAdmin;
    private Map<String, MBeanServerConnection> connections = new HashMap<String, MBeanServerConnection>();

    public HBaseMetrics(HBaseAdmin hbaseAdmin) throws MasterNotRunningException {
        this.hbaseAdmin = hbaseAdmin;
    }

    public int getBlockCacheHitRatio(String serverName) throws Exception {
        MBeanServerConnection connection = connections.get(serverName);
        if (connection == null) {
            String hostport = serverName + ":10102";
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
            JMXConnector connector = JMXConnectorFactory.connect(url);
            connection = connector.getMBeanServerConnection();
            connections.put(serverName, connection);
        }

        Integer blockCacheHitRatio = (Integer)connection.getAttribute(
                new ObjectName("hadoop:service=RegionServer,name=RegionServerStatistics"),
                "blockCacheHitRatio");

        return blockCacheHitRatio.intValue();
    }

    public void reportBlockCacheHitRatio(Metrics metrics) throws Exception {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (HServerInfo serverInfo : clusterStatus.getServerInfo()) {
            String serverName = serverInfo.getHostname();
            int ratio = getBlockCacheHitRatio(serverName);
            metrics.increment("blockCacheHitRatio", ratio);
        }
    }

    public Collection<TableInfo> getHBaseTableInfo(ClusterStatus clusterStatus) throws IOException {
        SortedMap<String, TableInfo> tableInfos = new TreeMap<String, TableInfo>();

        for (HServerInfo serverInfo : clusterStatus.getServerInfo()) {
            for (HServerLoad.RegionLoad regionLoad : serverInfo.getLoad().getRegionsLoad()) {
                String regionName = regionLoad.getNameAsString();
                int commaPos = regionName.indexOf(',');
                String tableName = regionName.substring(0, commaPos);

                TableInfo table = tableInfos.get(tableName);
                if (table == null) {
                    table = new TableInfo();
                    table.name = tableName;
                    tableInfos.put(tableName, table);
                }

                table.stores = Math.max(table.stores, regionLoad.getStores());
                table.regions++;
                table.storefiles += regionLoad.getStorefiles();
                table.storefilesMB += regionLoad.getStorefileSizeMB();
                table.memStoreMB += regionLoad.getMemStoreSizeMB();
            }
        }

        for (Map.Entry<String, TableInfo> entry : tableInfos.entrySet()) {
            HTableInterface table = new HTable(entry.getKey());
            Scan scan = new Scan();
            scan.setCaching(100);
            scan.setCacheBlocks(false);
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner scanner = table.getScanner(scan);
            int rowCount = 0;
            while (scanner.next() != null && rowCount <= 1000) {
                rowCount++;
            }

            entry.getValue().rows = rowCount;
            table.close();
        }

        return tableInfos.values();
    }

    public static class TableInfo {
        String name;
        int stores;
        int regions;
        int storefiles;
        int storefilesMB;
        int memStoreMB;
        int rows;
    }

    public void printFormattedHBaseState(PrintStream stream) throws IOException {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        Collection<TableInfo> tableInfos = getHBaseTableInfo(clusterStatus);

        stream.println("+--------------------------------------------------------------------------------------------------+");
        stream.println("| HBase version: " + clusterStatus.getHBaseVersion());
        stream.println("| # regions in transation: " + clusterStatus.getRegionsInTransition().size());
        stream.println("| The information below is from summarizing HBaseAdmin.ClusterStatus (= only online regions)");
        stream.println("+--------------------+----------+-----------+--------------+---------------+-------------+---------+");
        stream.println("| Table name         | # stores | # regions | # storefiles | storefiles MB | memstore MB | # rows  |");
        stream.println("+--------------------+----------+-----------+--------------+---------------+-------------+---------+");

        for (TableInfo tableInfo : tableInfos) {
            String rowCount = tableInfo.rows < 1000 ? String.valueOf(tableInfo.rows) : "> 1000";

            stream.printf("|%1$-20.20s|%2$10d|%3$11d|%4$14d|%5$15d|%6$13d|%7$9s|\n",
                    tableInfo.name, tableInfo.stores, tableInfo.regions, tableInfo.storefiles, tableInfo.storefilesMB,
                    tableInfo.memStoreMB, rowCount);
        }

        stream.println("+--------------------+----------+-----------+--------------+---------------+-------------+---------+");
    }
}
