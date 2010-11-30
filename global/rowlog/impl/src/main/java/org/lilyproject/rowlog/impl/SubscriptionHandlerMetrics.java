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

import javax.management.ObjectName;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

public class SubscriptionHandlerMetrics implements Updater {
    private final String subscriptionId;
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final ProcessorMetricsMXBean mbean;
    private final MetricsContext context;

    public MetricsTimeVaryingInt successRate = new MetricsTimeVaryingInt("success_rate", registry);
    public MetricsTimeVaryingInt failureRate = new MetricsTimeVaryingInt("failure_rate", registry);
    public MetricsIntValue queueSize = new MetricsIntValue("queueSize", registry);

    public SubscriptionHandlerMetrics(String subscriptionId) {
        this.subscriptionId = subscriptionId;
        context = MetricsUtil.getContext("subscriptionHandler");
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
            super(registry, "Lily Row Log SubscriptionHandler");

            mbeanName = MBeanUtil.registerMBean("Row Log SubscriptionHandler", subscriptionId, this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
