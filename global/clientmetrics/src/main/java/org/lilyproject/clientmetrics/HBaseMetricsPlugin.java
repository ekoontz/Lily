package org.lilyproject.clientmetrics;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.util.Collections;
import java.util.List;

public class HBaseMetricsPlugin implements MetricsPlugin {
    private HBaseMetrics hbaseMetrics;
    private HBaseAdmin hbaseAdmin;
    private boolean useJmx;

    public HBaseMetricsPlugin(HBaseMetrics hbaseMetrics, HBaseAdmin hbaseAdmin, boolean useJmx) throws MasterNotRunningException {
        this.hbaseAdmin = hbaseAdmin;
        this.hbaseMetrics = hbaseMetrics;
        this.useJmx = useJmx;
    }

    public void beforeReport(Metrics metrics) {
        if (!useJmx)
            return;

        try {
            hbaseMetrics.reportBlockCacheHitRatio(metrics);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public List<String> getExtraInfoLines() {
        try {
            ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();

            double avgLoad = clusterStatus.getAverageLoad();
            int deadServers = clusterStatus.getDeadServers();
            int liveServers = clusterStatus.getServers();
            int regionCount = clusterStatus.getRegionsCount();

            String line = String.format("HBase cluster status: avg load: %1$.2f, dead servers: %2$d, live servers: %3$d, regions: %4$d",
                    avgLoad, deadServers, liveServers, regionCount);

            return Collections.singletonList(line);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return Collections.emptyList();
    }
}
