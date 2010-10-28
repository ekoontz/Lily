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
package org.lilyproject.tools.mboximport;

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

