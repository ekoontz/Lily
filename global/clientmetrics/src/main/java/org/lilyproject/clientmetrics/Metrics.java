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
    private int threadCount = 1;
    private Table table;

    public Metrics() {
        this(System.out, null);
    }

    public Metrics(File file, MetricsPlugin plugin) throws FileNotFoundException {
        this(new PrintStream(new FileOutputStream(file)), plugin);
    }

    public Metrics(PrintStream reportStream, MetricsPlugin plugin) {
        this.reportStream = reportStream;
        this.plugin = plugin == null ? new NullPlugin() : plugin;

        table = new Table(reportStream);
        table.addColumn(40, "Name", "s");
        table.addColumn(-1, "Op count", "d");
        table.addColumn(-1, "Average", "f");
        table.addColumn(-1, "Median", "f");
        table.addColumn(-1, "Minimum", "f");
        table.addColumn(-1, "Maximum", "f");
        table.addColumn(-1, "Alltime ops", "d");
        table.addColumn(-1, "Alltime avg", "f");
        table.finishDefinition();
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void finish() {
        // print one last report
        printReport();
        printGlobalOpsPerSec();
    }

    /**
     * Does an in-between reset of the metrics. This is useful if your test consists of multiple phases
     * which each have different metrics.
     */
    public synchronized void restart() {
        printReport();
        printGlobalOpsPerSec();
        metrics.clear();
        intervalStartedAt = null;
        startedAt = null;

        reportStream.println("");
        reportStream.println("Metrics have been reset");
        reportStream.println("");
    }

    public void printPhaseTitle(String title) {
        reportStream.print("+");
        for (int i = 0; i < title.length() + 2; i++) {
            reportStream.print("-");
        }
        reportStream.println("+");
        reportStream.println("| " + title + " |");
        reportStream.print("+");
        for (int i = 0; i < title.length() + 2; i++) {
            reportStream.print("-");
        }
        reportStream.println("+");
    }

    public int getIntervalDuration() {
        return intervalDuration;
    }

    public synchronized void increment(String name, double value) {
        increment(name, null, 1, value);
    }

    public synchronized void increment(String name, String type, double value) {
        increment(name, type, 1, value);
    }

    /**
     *
     * @param type optional, can be null. If specified, number of operations per second will be summarized
     *             for all metrics of the same type. The value must represent a timing in milliseconds.
     */
    public synchronized void increment(String name, String type, int operations, double value) {
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
            metric.type = type;
            metrics.put(name, metric);
        }

        metric.add(operations, value);
    }

    public void printReport() {
        if (intervalStartedAt == null) {
            return;
        }

        inReport = true;

        plugin.beforeReport(this);

        long now = System.currentTimeMillis();
        long actualIntervalDuration = now - intervalStartedAt.getMillis();

        table.fullSepLine();
        table.crossColumn("Interval started at: " + intervalStartedAt + " (duration: " + (actualIntervalDuration / 1000) + "s).");
        table.crossColumn("Measurements started at: " + startedAt + " (duration: " + formatDuration(now - startedAt.getMillis()) + ")");


        List<String> extra = plugin.getExtraInfoLines();
        for (String line : extra) {
            table.crossColumn(line);
        }

        table.columnSepLine();
        table.titles();
        table.columnSepLine();

        Map<String, CountAndValue> statByType = new TreeMap<String, CountAndValue>();

        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            String name = entry.getKey();
            Metric metric = entry.getValue();

            if (metric.type != null) {
                name = metric.type + ":" + name;
                CountAndValue stat = statByType.get(metric.type);
                if (stat == null) {
                    stat = new CountAndValue();
                    statByType.put(metric.type, stat);
                }

                stat.count += metric.getIntervalCount();
                stat.value += metric.getIntervalValue();
            }

            table.columns(name, metric.getIntervalCount(), metric.getIntervalAverage(), metric.getIntervalMedian(),
                    metric.getIntervalMin(), metric.getIntervalMax(), metric.getAllTimeCount(),
                    metric.getAllTimeAverage());
        }
        table.columnSepLine();


        if (statByType.size() > 0) {
            int i = 0;
            for (Map.Entry<String, CountAndValue> entry : statByType.entrySet()) {
                if (entry.getValue().count == 0)
                    continue;

                i++;

                // The real time spent in the operations is the time counted together from multiple threads,
                // hence more than the actual elapsed time. E.g. in an interval of 30s, each thread runs 30s,
                // hence with 3 threads there are 90 seconds spent. So usually the interval ops/sec will give
                // a better picture.
                double opsPerSec = (((double)entry.getValue().count) / (entry.getValue().value)) * 1000d;
                double opsPerSecInt = (((double)entry.getValue().count) / ((double)actualIntervalDuration)) * 1000d;

                table.crossColumn(String.format("%1$s ops/sec: %5$.2f real (=%3$.2fx%4$d), %2$.2f interval",
                        entry.getKey(), opsPerSecInt, opsPerSec, threadCount, opsPerSec * ((double)threadCount)));
            }
            if (i > 0) {
                table.fullSepLine();
            }
        }

        reportStream.flush();

        inReport = false;
    }

    private void printGlobalOpsPerSec() {
        if (startedAt == null)
            return;

        double testDuration = System.currentTimeMillis() - startedAt.getMillis();

        Map<String, CountAndValue> statByType = new TreeMap<String, CountAndValue>();

        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            Metric metric = entry.getValue();

            if (metric.type != null) {
                CountAndValue stat = statByType.get(metric.type);
                if (stat == null) {
                    stat = new CountAndValue();
                    statByType.put(metric.type, stat);
                }

                stat.count += metric.getAllTimeCount();
                stat.value += metric.getAllTimeValue();
            }
        }

        if (statByType.size() > 0) {
            reportStream.println();
            reportStream.println("Global ops/sec of the test:");
            for (Map.Entry<String, CountAndValue> entry : statByType.entrySet()) {
                if (entry.getValue().count == 0)
                    continue;

                double opsPerSec = (((double)entry.getValue().count) / (entry.getValue().value)) * 1000d;
                double opsPerSecInt = (((double)entry.getValue().count) / (testDuration)) * 1000d;

                reportStream.printf("%1$s ops/sec: %5$.2f real (=%3$.2fx%4$d), %2$.2f interval\n", entry.getKey(),
                        opsPerSecInt, opsPerSec, threadCount, opsPerSec * ((double)threadCount));
            }
            reportStream.println();
        }
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
        private List<Double> values = new ArrayList<Double>(1000);

        String type;
        
        int intervalCount;
        double intervalValue;
        double intervalMin;
        double intervalMax;

        int allTimeCount;
        double allTimeValue;

        public Metric() {
            rollInterval();
        }

        /**
         *
         * @param value will most often be a duration in ms, but could be other kinds of values as well.
         */
        public void add(int operations, double value) {
            if (operations == 0)
                return;

            intervalCount += operations;
            intervalValue += value;
            allTimeCount += operations;
            allTimeValue += value;

            double valuePerOp = value / (double)operations;

            if (valuePerOp < intervalMin)
                intervalMin = valuePerOp;

            if (valuePerOp > intervalMax)
                intervalMax = valuePerOp;

            values.add(valuePerOp);
        }

        public void rollInterval() {
            intervalCount = 0;
            intervalValue = 0;
            intervalMin = Integer.MAX_VALUE;
            intervalMax = 0;
            values.clear();
        }

        public int getIntervalCount() {
            return intervalCount;
        }

        public double getIntervalValue() {
            return intervalValue;
        }

        public int getAllTimeCount() {
            return allTimeCount;
        }

        public double getIntervalAverage() {
            return intervalCount == 0 ? 0 : intervalValue / (double)intervalCount;
        }

        public double getAllTimeAverage() {
            return allTimeCount == 0 ? 0 : allTimeValue / (double)allTimeCount;
        }

        public double getAllTimeValue() {
            return allTimeValue;
        }

        public double getIntervalMin() {
            return intervalCount == 0 ? 0 : intervalMin;
        }

        public double getIntervalMax() {
            return intervalMax;
        }

        public double getIntervalMedian() {
            if (values.size() == 0)
                return 0;

            Collections.sort(values);
            int middle = values.size() / 2;

            if (values.size() % 2 == 1) {
                return values.get(middle);
            } else {
                return (values.get(middle - 1) + values.get(middle)) / 2d;
            }
        }
    }

    private static class CountAndValue {
        int count;
        double value;
    }
}
