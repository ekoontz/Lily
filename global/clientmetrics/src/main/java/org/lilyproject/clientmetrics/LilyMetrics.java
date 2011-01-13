package org.lilyproject.clientmetrics;

import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.io.PrintStream;
import java.util.List;

public class LilyMetrics {
    private ZooKeeperItf zk;
    private JmxConnections jmxConnections = new JmxConnections();
    private static final String LILY_JMX_PORT = "10202";

    public LilyMetrics(ZooKeeperItf zk) {
        this.zk = zk;
    }

    public void outputLilyServerInfo(PrintStream ps) throws Exception {
        Table table = new Table(ps);
        table.addColumn(30, "Lily server", "s");
        table.addColumn(10, "Version", "s");
        table.addColumn(-1, "IndexerMaster", "s");
        table.addColumn(-1, "MQ proc", "s");
        table.addColumn(-1, "WAL proc", "s");
        table.addColumn(-1, "Max heap", "f");
        table.addColumn(-1, "Used heap", "f");

        table.finishDefinition();

        List<String> lilyServers = zk.getChildren("/lily/repositoryNodes", false);

        table.fullSepLine();
        table.crossColumn("Information on the Lily servers");
        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        ObjectName lily = new ObjectName("Lily:name=Info");
        ObjectName memory = new ObjectName("java.lang:type=Memory");

        for (String server : lilyServers) {
            int colonPos = server.indexOf(':');
            String address = server.substring(0, colonPos);

            MBeanServerConnection connection = jmxConnections.getMBeanServer(address, LILY_JMX_PORT);
            String version = (String)connection.getAttribute(lily, "Version");
            boolean indexerMaster = (Boolean)connection.getAttribute(lily, "IndexerMaster");
            boolean mqProcessor = (Boolean)connection.getAttribute(lily, "RowLogProcessorMQ");
            boolean walProcessor = (Boolean)connection.getAttribute(lily, "RowLogProcessorWAL");

            CompositeDataSupport heapMemUsage = (CompositeDataSupport)connection.getAttribute(memory, "HeapMemoryUsage");
            double maxHeapMB = ((double)(Long)heapMemUsage.get("max")) / 1024d / 1024d;
            double usedHeapMB = ((double)(Long)heapMemUsage.get("used")) / 1024d / 1024d;

            table.columns(address, version, String.valueOf(indexerMaster), String.valueOf(mqProcessor),
                    String.valueOf(walProcessor), maxHeapMB, usedHeapMB);
        }

        table.columnSepLine();
    }
}
