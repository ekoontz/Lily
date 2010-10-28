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
package org.lilyproject.util.hbase.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;

/**
 * Metric for a value which is counted during a reporting interval.
 * Note that in monitoring tools, the total value  
 */
public class MetricsIntervalLongValue extends MetricsBase {

    private final Log log = LogFactory.getLog(getClass());

    private long value;

    private long prevValue;

    public MetricsIntervalLongValue(final String name, MetricsRegistry registry, final String description) {
        super(name, description);
        value = 0;
        registry.add(name, this);
    }

    public MetricsIntervalLongValue(final String name, MetricsRegistry registry) {
        this(name, registry, NO_DESCRIPTION);
    }

    public synchronized void inc(final long value) {
        this.value += value;
    }

    public synchronized void inc() {
        this.value += 1;
    }

    private synchronized void intervalHeartBeat() {
        prevValue = value;
        value = 0;
    }

    public synchronized void pushMetric(final MetricsRecord mr) {
        intervalHeartBeat();
        try {
            mr.setMetric(getName(), prevValue);
        } catch (Exception e) {
            log.info("pushMetric failed for " + getName(), e);
        }
    }


    public synchronized float getPreviousIntervalValue() {
        return prevValue;
    }
}

