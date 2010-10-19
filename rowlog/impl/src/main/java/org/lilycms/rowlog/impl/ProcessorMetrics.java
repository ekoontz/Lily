package org.lilycms.rowlog.impl;

import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.lilycms.util.hbase.metrics.MBeanUtil;
import org.lilycms.util.hbase.metrics.MetricsDynamicMBeanBase;
import org.lilycms.util.hbase.metrics.MetricsNonTimeRate;

import javax.management.ObjectName;

public class ProcessorMetrics implements Updater {
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final ProcessorMetricsMXBean mbean;
    private final MetricsContext context;

    public MetricsRate scans = new MetricsRate("scans", registry);

    public MetricsNonTimeRate messagesPerScan = new MetricsNonTimeRate("messagesPerScan", registry);

    public MetricsRate wakeups = new MetricsRate("wakeups", registry);

    public ProcessorMetrics(String subscriptionId) {
        context = MetricsUtil.getContext("lily");
        metricsRecord = MetricsUtil.createRecord(context, "rowLogProcessor." + subscriptionId);
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

            mbeanName = MBeanUtil.registerMBean("Row Log Processor", "Metrics", this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
