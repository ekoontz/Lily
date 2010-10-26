package org.lilyproject.repository.impl;

import java.util.EnumMap;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

public class RepositoryMetrics implements Updater {
    public enum Action{CREATE, READ, UPDATE, DELETE};
    public enum HBaseAction{PUT, GET, LOCK, UNLOCK};
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final MetricsContext context;
    private final EnumMap<Action, MetricsTimeVaryingRate> rates = new EnumMap<Action, MetricsTimeVaryingRate>(Action.class);
    private final EnumMap<HBaseAction, MetricsTimeVaryingRate> hbaseRates = new EnumMap<HBaseAction, MetricsTimeVaryingRate>(HBaseAction.class);
    private final RepositoryMetricsMXBean mbean;
    private final String recordName;

    public RepositoryMetrics(String recordName) {
        this.recordName = recordName;
        for (Action action : Action.values()) {
            rates.put(action, new MetricsTimeVaryingRate(action.name().toLowerCase(), registry));
        }

        for (HBaseAction action : HBaseAction.values()) {
            hbaseRates.put(action, new MetricsTimeVaryingRate(action.name().toLowerCase(), registry));
        }
        
        context = MetricsUtil.getContext("repository");
        metricsRecord = MetricsUtil.createRecord(context, recordName);
        context.registerUpdater(this);
        mbean = new RepositoryMetricsMXBean(this.registry);
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

    synchronized void report(Action action, long duration) {
        rates.get(action).inc(duration);
    }

    synchronized void reportHBase(HBaseAction action, long duration) {
        hbaseRates.get(action).inc(duration);
    }

    public class RepositoryMetricsMXBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public RepositoryMetricsMXBean(MetricsRegistry registry) {
            super(registry, "Lily Repository");

            mbeanName = MBeanUtil.registerMBean("Repository", recordName, this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
