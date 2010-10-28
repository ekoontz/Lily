/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
