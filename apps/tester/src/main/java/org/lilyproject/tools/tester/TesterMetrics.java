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
package org.lilyproject.tools.tester;

import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.lilyproject.util.hbase.metrics.MBeanUtil;
import org.lilyproject.util.hbase.metrics.MetricsDynamicMBeanBase;

import javax.management.ObjectName;
import java.util.EnumMap;

public class TesterMetrics implements Updater {
    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;
    private final MetricsContext context;
    private final EnumMap<Action, MetricsTimeVaryingRate> rates = new EnumMap<Action, MetricsTimeVaryingRate>(Action.class);
    private final EnumMap<Action, MetricsRate> opcount = new EnumMap<Action, MetricsRate>(Action.class);
    private final MetricsLongValue failures = new MetricsLongValue("failures", registry);
    private final TesterMetricsMBean mbean;

    public TesterMetrics(String recordName) {
        for (Action action : Action.values()) {
            rates.put(action, new MetricsTimeVaryingRate(action.name().toLowerCase(), registry));
            opcount.put(action, new MetricsRate("avg_ops_"+action.name().toLowerCase(), registry));
        }
        
        context = MetricsUtil.getContext("tester");
        metricsRecord = MetricsUtil.createRecord(context, recordName);
        context.registerUpdater(this);
        mbean = new TesterMetricsMBean(this.registry);
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

    synchronized void report(Action action, boolean success, int duration) {
        if (!success) {
            failures.set(failures.get() + 1);
        }

        rates.get(action).inc(duration);
        opcount.get(action).inc();
    }

    public class TesterMetricsMBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public TesterMetricsMBean(MetricsRegistry registry) {
            super(registry, "Lily Tester");

            mbeanName = MBeanUtil.registerMBean("Tester", "Metrics", this);
        }

        public void shutdown() {
            if (mbeanName != null)
                MBeanUtil.unregisterMBean(mbeanName);
        }
    }
}
