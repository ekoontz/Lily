package org.lilyproject.clientmetrics;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;

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
}
