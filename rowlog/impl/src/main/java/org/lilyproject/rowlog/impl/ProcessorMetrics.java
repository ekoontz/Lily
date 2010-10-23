package org.lilyproject.rowlog.impl;

import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;
import org.lilyproject.util.hbase.metrics.MetricsNonTimeRate;

import javax.management.ObjectName;

public class ProcessorMetrics implements Updater {
    private final String subscriptionId;
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final ProcessorMetricsMXBean mbean;
    private final MetricsContext context;

    public MetricsRate scans = new MetricsRate("scans_rate", registry);

    public MetricsNonTimeRate messagesPerScan = new MetricsNonTimeRate("messagesPerScan", registry);

    public MetricsRate wakeups = new MetricsRate("wakeups_rate", registry);

    public ProcessorMetrics(String subscriptionId) {
        this.subscriptionId = subscriptionId;
        context = MetricsUtil.getContext("rowlog");
        metricsRecord = MetricsUtil.createRecord(context, subscriptionId);
        context.registerUpdater(this);
        mbean = new ProcessorMetricsMXBean(this.registry);
    }

    public void shutdown() {
        context.unregisterUpdater(this);
        mbean.shutdown();
    }

    public void doUpdates(MetricsContext metricsContext) {
        synchronized (this) {
          for (MetricsBase m : registry.getMetricsList()) {
            m.pushMetric(metricsRecord);
          }
        }
        metricsRecord.update();
    }

    public class ProcessorMetricsMXBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public ProcessorMetricsMXBean(MetricsRegistry registry) {
            super(registry, "Lily Row Log Processor");

            mbeanName = MBeanUtil.registerMBean("Row Log Processor", subscriptionId, this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
