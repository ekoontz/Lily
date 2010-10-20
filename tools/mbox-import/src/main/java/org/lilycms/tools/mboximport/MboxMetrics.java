package org.lilycms.tools.mboximport;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.lilycms.util.hbase.metrics.MBeanUtil;
import org.lilycms.util.hbase.metrics.MetricsDynamicMBeanBase;

import javax.management.ObjectName;

public class MboxMetrics implements Updater {
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final MetricsContext context;
    public final MetricsTimeVaryingRate messages = new MetricsTimeVaryingRate("messages", registry);
    public final MetricsTimeVaryingRate parts = new MetricsTimeVaryingRate("parts", registry);
    public final MetricsTimeVaryingRate blobs = new MetricsTimeVaryingRate("blobs", registry);
    private final MboxMetricsMBean mbean;

    public MboxMetrics(String recordName) {
        context = MetricsUtil.getContext("mbox");
        metricsRecord = MetricsUtil.createRecord(context, recordName);
        context.registerUpdater(this);
        mbean = new MboxMetricsMBean(this.registry);
    }

    public void shutdown() {
        context.unregisterUpdater(this);
        mbean.shutdown();
    }

    public synchronized void doUpdates(MetricsContext unused) {
        synchronized (this) {
          for (MetricsBase m : registry.getMetricsList()) {
            m.pushMetric(metricsRecord);
          }
        }
        metricsRecord.update();
    }

    public class MboxMetricsMBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public MboxMetricsMBean(MetricsRegistry registry) {
            super(registry, "Lily mbox import");

            mbeanName = MBeanUtil.registerMBean("mbox import", "Metrics", this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}

