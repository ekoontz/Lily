package org.lilyproject.indexer.engine;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

import javax.management.ObjectName;

public class IndexUpdaterMetrics implements Updater {
    private final String indexName;
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final MetricsContext context;
    private final IndexerMetricsMBean mbean;

    public MetricsTimeVaryingRate updates = new MetricsTimeVaryingRate("updates", registry);

    public IndexUpdaterMetrics(String indexName) {
        this.indexName = indexName;
        context = MetricsUtil.getContext("indexUpdater");
        metricsRecord = MetricsUtil.createRecord(context, indexName);
        context.registerUpdater(this);
        mbean = new IndexerMetricsMBean(this.registry);
    }

    public void shutdown() {
        mbean.shutdown();
        context.unregisterUpdater(this);
    }

    public void doUpdates(MetricsContext metricsContext) {
        synchronized (this) {
          for (MetricsBase m : registry.getMetricsList()) {
            m.pushMetric(metricsRecord);
          }
        }
        metricsRecord.update();
    }

    public class IndexerMetricsMBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public IndexerMetricsMBean(MetricsRegistry registry) {
            super(registry, "Lily Index Updater");

            mbeanName = MBeanUtil.registerMBean("Index Updater", indexName, this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
