package org.lilyproject.clientmetrics;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.net.MalformedURLException;
import java.util.*;

public class HBaseMetrics {
    private HBaseAdmin hbaseAdmin;
    private JmxConnections jmxConnections = new JmxConnections();
    private static final String HBASE_JMX_PORT = "10102";

    private ObjectName regionServerStats;
    private ObjectName operationSystem;

    public HBaseMetrics(HBaseAdmin hbaseAdmin) throws MasterNotRunningException, MalformedObjectNameException {
        this.hbaseAdmin = hbaseAdmin;
        this.regionServerStats = new ObjectName("hadoop:service=RegionServer,name=RegionServerStatistics");
        this.operationSystem = new ObjectName("java.lang:type=OperatingSystem");
    }

    public void reportMetrics(Metrics metrics) throws Exception {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (HServerInfo serverInfo : clusterStatus.getServerInfo()) {
            String serverName = serverInfo.getHostname();

            MBeanServerConnection connection = jmxConnections.getMBeanServer(serverName, HBASE_JMX_PORT);

            Integer blockCacheHitRatio = (Integer)connection.getAttribute(regionServerStats, "blockCacheHitRatio");
            Double sysLoadAvg = (Double)connection.getAttribute(operationSystem, "SystemLoadAverage");


            metrics.increment("blockCacheHitRatio:" + serverName, blockCacheHitRatio);
            metrics.increment("sysLoadAvg:" + serverName, sysLoadAvg);
        }
    }

    public Collection<TableInfo> getHBaseTableInfo(ClusterStatus clusterStatus) throws IOException {
        SortedMap<String, TableInfo> tableInfos = new TreeMap<String, TableInfo>();

        for (HServerInfo serverInfo : clusterStatus.getServerInfo()) {
            for (HServerLoad.RegionLoad regionLoad : serverInfo.getLoad().getRegionsLoad()) {
                String regionName = regionLoad.getNameAsString();
                String tableName = getTableNameFromRegionName(regionName);

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

    private String getTableNameFromRegionName(String regionName) {
        int commaPos = regionName.indexOf(',');
        String tableName = regionName.substring(0, commaPos);
        return tableName;
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

    public void outputHBaseState(PrintStream stream) throws IOException {
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        Collection<TableInfo> tableInfos = getHBaseTableInfo(clusterStatus);

        Table table = new Table(stream);
        table.addColumn(20, "Table name", "s");
        table.addColumn(-1, "# stores", "d");
        table.addColumn(-1, "# regions", "d");
        table.addColumn(-1, "# storesfiles", "d");
        table.addColumn(-1, "storefiles MB", "d");
        table.addColumn(-1, "memstore MB", "d");
        table.addColumn(-1, "# rows", "s", false, 0);
        table.finishDefinition();

        table.fullSepLine();
        table.crossColumn("HBase version: " + clusterStatus.getHBaseVersion());
        table.crossColumn("# regions in transation: " + clusterStatus.getRegionsInTransition().size());
        table.crossColumn("The information below is from summarizing HBaseAdmin.ClusterStatus (= only online regions)");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        for (TableInfo tableInfo : tableInfos) {
            String rowCount = tableInfo.rows < 1000 ? String.valueOf(tableInfo.rows) : "> 1000";

            table.columns(tableInfo.name, tableInfo.stores, tableInfo.regions, tableInfo.storefiles,
                    tableInfo.storefilesMB, tableInfo.memStoreMB, rowCount);
        }

        table.columnSepLine();
    }

    public void outputRegionServersInfo(PrintStream ps) throws Exception {
        Table table = new Table(ps);
        table.addColumn(30, "Regionserver", "s");
        table.addColumn(6, "Arch", "s");
        table.addColumn(-1, "# CPU", "d");
        table.addColumn(15, "OS Version", "s");
        table.addColumn(-1, "Physical mem", "f");
        table.addColumn(-1, "JVM version", "s");
        table.addColumn(-1, "Max heap", "f");
        table.addColumn(-1, "Used heap", "f");
        table.finishDefinition();

        table.fullSepLine();
        table.crossColumn("Information on the HBase region servers");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        ObjectName runtime = new ObjectName("java.lang:type=Runtime");
        ObjectName memory = new ObjectName("java.lang:type=Memory");

        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (HServerInfo serverInfo : clusterStatus.getServerInfo()) {
            String hostname = serverInfo.getHostname();
            MBeanServerConnection connection = jmxConnections.getMBeanServer(hostname, HBASE_JMX_PORT);

            String arch = (String)connection.getAttribute(operationSystem, "Arch");
            Integer processors = (Integer)connection.getAttribute(operationSystem, "AvailableProcessors");
            String osVersion = (String)connection.getAttribute(operationSystem, "Version");
            long physicalMem = (Long)connection.getAttribute(operationSystem, "TotalPhysicalMemorySize");
            double physicalMemMB = ((double)physicalMem) / 1024d / 1024d;

            String vmVersion = (String)connection.getAttribute(runtime, "VmVersion");

            CompositeDataSupport heapMemUsage = (CompositeDataSupport)connection.getAttribute(memory, "HeapMemoryUsage");
            double maxHeapMB = ((double)(Long)heapMemUsage.get("max")) / 1024d / 1024d;
            double usedHeapMB = ((double)(Long)heapMemUsage.get("used")) / 1024d / 1024d;

            table.columns(hostname, arch, processors, osVersion, physicalMemMB, vmVersion, maxHeapMB, usedHeapMB);
        }

        table.columnSepLine();
    }

    public void outputRegionCountByServer(PrintStream ps) throws IOException {
        Table table = new Table(ps);
        table.addColumn(30, "Regionserver", "s");
        table.addColumn(30, "Table name", "s");
        table.addColumn(-1, "Region count", "d");
        table.finishDefinition();

        table.fullSepLine();
        table.crossColumn("Number of regions for each table on each region server");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        for (HServerInfo serverInfo : clusterStatus.getServerInfo()) {
            Map<String, Integer> regionCounts = new HashMap<String, Integer>();
            for (HServerLoad.RegionLoad regionLoad : serverInfo.getLoad().getRegionsLoad()) {
                String tableName = getTableNameFromRegionName(regionLoad.getNameAsString());
                if (regionCounts.containsKey(tableName)) {
                    regionCounts.put(tableName, regionCounts.get(tableName) + 1);
                } else {
                    regionCounts.put(tableName, 1);
                }
            }

            String hostName = serverInfo.getHostname();

            for (Map.Entry<String, Integer> entry : regionCounts.entrySet()) {
                table.columns(hostName, entry.getKey(), entry.getValue());
            }
        }

        table.columnSepLine();
    }
}
