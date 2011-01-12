package org.lilyproject.testclientfw;

import org.apache.commons.logging.LogFactory;

/**
 * Class to measure elapsed time in milliseconds (but measured at nanosecond precision).
 * One thread can only do one time-measurement at a time (it makes use of threadlocal state).
 */
public class Stopwatch {
    private static final ThreadLocal<StopwatchData> dataTL = new ThreadLocal<StopwatchData>() {
        @Override
        protected StopwatchData initialValue() {
            return new StopwatchData();
        }
    };

    public static void start() {
        StopwatchData data = dataTL.get();

        if (data.started) {
            LogFactory.getLog(Stopwatch.class).error("Stopwatch.start() called but it was already started.");
        }

        data.startedAt = System.nanoTime();
        data.started = true;
    }

    public static double stop() {
        StopwatchData data = dataTL.get();

        if (!data.started) {
            LogFactory.getLog(Stopwatch.class).error("Stopwatch.stop() called but it was not started.");
        }

        double duration = (((double)System.nanoTime()) - ((double)data.startedAt)) / 1e6;
        data.started = false;
        return duration;
    }

    private static class StopwatchData {
        long startedAt;
        boolean started;
    }
}
