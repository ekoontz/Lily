package org.lilycms.indexer.engine;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.lilycms.util.hbase.metrics.MBeanUtil;
import org.lilycms.util.hbase.metrics.MetricsDynamicMBeanBase;

import javax.management.ObjectName;

public class IndexerMetrics implements Updater {
    private final String indexName;
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final IndexerMetricsMBean mbean;
    private final MetricsContext context;

    public MetricsTimeVaryingLong adds = new MetricsTimeVaryingLong("adds", registry);

    public MetricsTimeVaryingLong deletesById = new MetricsTimeVaryingLong("deletesById", registry);

    public MetricsTimeVaryingLong deletesByQuery = new MetricsTimeVaryingLong("deletesByQuery", registry);

    public IndexerMetrics(String indexName) {
        this.indexName = indexName;
        context = MetricsUtil.getContext("indexer");
        metricsRecord = MetricsUtil.createRecord(context, indexName);
        context.registerUpdater(this);
        mbean = new IndexerMetricsMBean(this.registry);
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

    public class IndexerMetricsMBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public IndexerMetricsMBean(MetricsRegistry registry) {
            super(registry, "Lily Indexer");

            mbeanName = MBeanUtil.registerMBean("Indexer", indexName, this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
