package org.lilyproject.clientmetrics;

import org.joda.time.DateTime;

import java.io.*;
import java.util.*;

/**
 * Keeps metrics for a certain interval and prints them out when the interval ends.
 */
public class Metrics {
    private Map<String, Metric> metrics = new TreeMap<String, Metric>();
    private int intervalDuration = 1000 * 30;
    private DateTime intervalStartedAt;
    private DateTime startedAt;
    private PrintStream reportStream;
    private MetricsPlugin plugin;
    private boolean inReport;

    public Metrics(File file, MetricsPlugin plugin) throws FileNotFoundException {
        this(new PrintStream(new FileOutputStream(file)), plugin);
    }

    public Metrics(PrintStream reportStream, MetricsPlugin plugin) {
        this.reportStream = reportStream;
        this.plugin = plugin == null ? new NullPlugin() : plugin;
    }

    public Metrics() {
        this(System.out, null);
    }

    public void finish() {
        // print one last report
        printReport();
        reportStream.close();
    }

    public synchronized void increment(String name, int duration) {
        if (intervalStartedAt == null) {
            // it's our very first value
            intervalStartedAt = new DateTime();
            startedAt = new DateTime();
        }

        if (!inReport && System.currentTimeMillis() - intervalStartedAt.getMillis() >= intervalDuration) {
            printReport();
            for (Metric metric : metrics.values()) {
                metric.rollInterval();
            }
            intervalStartedAt = new DateTime();
        }

        Metric metric = metrics.get(name);
        if (metric == null) {
            metric = new Metric();
            metrics.put(name, metric);
        }

        metric.add(duration);
    }

    public void printReport() {
        if (intervalStartedAt == null) {
            return;
        }

        inReport = true;

        plugin.beforeReport(this);

        long now = System.currentTimeMillis();
        long actualIntervalDuration = now - intervalStartedAt.getMillis();

        reportStream.println("+-----------------------------------------------------------------------------------------------------------------------+");
        reportStream.println("| Interval started at: " + intervalStartedAt + " (duration: " + (actualIntervalDuration / 1000) + "s).");
        reportStream.println("| Measurements started at: " + startedAt + " (duration: " + formatDuration(now - startedAt.getMillis()) + ")");

        List<String> extra = plugin.getExtraInfoLines();
        for (String line : extra) {
            reportStream.println("| " + line);
        }

        reportStream.println("+------------------------------+----------+----------+----------+----------+----------+--------------+------------------+");
        reportStream.println("| Name                         | Op count | Average  | Median   | Minimum  | Maximum  | All time ops | All time average |");
        reportStream.println("+------------------------------+----------+----------+----------+----------+----------+--------------+------------------+");

        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            String name = entry.getKey();
            Metric metric = entry.getValue();

            reportStream.printf ("|%1$-30.30s|%2$10d|%3$10.2f|%4$10.2f|%5$10d|%6$10d|%7$14d|%8$18.2f|\n",
                    name, metric.getIntervalCount(), metric.getIntervalAverage(), metric.getIntervalMedian(),
                    metric.getIntervalMin(), metric.getIntervalMax(), metric.getAllTimeCount(),
                    metric.getAllTimeAverage());
        }

        reportStream.println("+------------------------------+----------+----------+----------+----------+----------+--------------+------------------+");
        reportStream.flush();

        inReport = false;
    }

    private String formatDuration(long millis) {
        long seconds = millis / 1000;

        long secondsOverflow = seconds % 60;

        long minutes = (seconds - secondsOverflow) / 60;

        long minutesOverflow = minutes % 60;

        long hours = (minutes - minutesOverflow) / 60;

        return String.format("%1$02d:%2$02d:%3$02d", hours, minutesOverflow, secondsOverflow);
    }

    private static class Metric {
        private List<Integer> durations = new ArrayList<Integer>(1000);

        int intervalCount;
        long intervalDuration;
        int intervalMin;
        int intervalMax;

        int allTimeCount;
        int allTimeDuration;

        public Metric() {
            rollInterval();
        }

        public void add(int duration) {
            intervalCount++;
            intervalDuration += duration;
            allTimeCount++;
            allTimeDuration += duration;

            if (duration < intervalMin)
                intervalMin = duration;

            if (duration > intervalMax)
                intervalMax = duration;

            durations.add(duration);
        }

        public void rollInterval() {
            intervalCount = 0;
            intervalDuration = 0;
            intervalMin = Integer.MAX_VALUE;
            intervalMax = 0;
            durations.clear();
        }

        public int getIntervalCount() {
            return intervalCount;
        }

        public int getAllTimeCount() {
            return allTimeCount;
        }

        public double getIntervalAverage() {
            return intervalCount == 0 ? 0 : (double)intervalDuration / (double)intervalCount;
        }

        public double getAllTimeAverage() {
            return allTimeCount == 0 ? 0 : (double)allTimeDuration / (double)allTimeCount;
        }

        public int getIntervalMin() {
            return intervalCount == 0 ? 0 : intervalMin;
        }

        public int getIntervalMax() {
            return intervalMax;
        }

        public double getIntervalMedian() {
            if (durations.size() == 0)
                return 0;

            Collections.sort(durations);
            int middle = durations.size() / 2;

            if (durations.size() % 2 == 1) {
                return durations.get(middle);
            } else {
                return (durations.get(middle - 1) + durations.get(middle)) / 2d;
            }
        }
    }
}
