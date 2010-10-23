package org.lilyproject.util.hbase.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;

public class MetricsNonTimeRate extends MetricsBase {

    private final Log log = LogFactory.getLog(getClass());

    private long value1;
    private long value2;

    private float prevRate;

    public MetricsNonTimeRate(final String name, MetricsRegistry registry, final String description) {
        super(name, description);
        value1 = 0;
        value2 = 0;
        registry.add(name, this);
    }

    public MetricsNonTimeRate(final String name, MetricsRegistry registry) {
        this(name, registry, NO_DESCRIPTION);
    }

    public synchronized void inc(final long value1) {
        this.value1 += value1;
        this.value2++;
    }

    public synchronized void inc(final long value1, final long value2) {
        this.value1 += value1;
        this.value2 += value2;
    }

    private synchronized void intervalHeartBeat() {
        prevRate = value2 > 0 ? (float)value1 / (float)value2 : 0f;
        value1 = 0;
        value2 = 0;
    }

    public synchronized void pushMetric(final MetricsRecord mr) {
        intervalHeartBeat();
        try {
            mr.setMetric(getName(), prevRate);
        } catch (Exception e) {
            log.info("pushMetric failed for " + getName(), e);
        }
    }


    public synchronized float getPreviousIntervalValue() {
        return prevRate;
    }
}

