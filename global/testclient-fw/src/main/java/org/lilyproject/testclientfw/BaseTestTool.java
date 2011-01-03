package org.lilyproject.testclientfw;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.clientmetrics.Metrics;
import org.lilyproject.clientmetrics.HBaseMetricsPlugin;
import org.lilyproject.clientmetrics.MetricsPlugin;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class BaseTestTool extends BaseZkCliTool {
    private static final int DEFAULT_WORKERS = 2;

    private Option workersOption;

    private Option verboseOption;

    private Option hbaseMetricsOption;

    protected int workers;

    protected boolean verbose;

    protected boolean hbaseMetrics;

    protected ThreadPoolExecutor executor;

    protected Metrics metrics;

    protected ZooKeeperItf zk;

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        workersOption = OptionBuilder
                .withArgName("count")
                .hasArg()
                .withDescription("Number of workers (threads)")
                .withLongOpt("workers")
                .create("w");
        options.add(workersOption);

        verboseOption = OptionBuilder
                .withDescription("Be verbose")
                .withLongOpt("verbose")
                .create("v");
        options.add(verboseOption);

        hbaseMetricsOption = OptionBuilder
                .withDescription("Enable HBase metrics options (requires JMX on default port 10102)")
                .withLongOpt("hbase-metrics")
                .create("m");
        options.add(hbaseMetricsOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0)
            return result;

        workers = Util.getIntOption(cmd, workersOption, DEFAULT_WORKERS);

        if (cmd.hasOption(verboseOption.getOpt())) {
            verbose = true;
        }

        if (cmd.hasOption(hbaseMetricsOption.getOpt())) {
            hbaseMetrics = true;
        }

        return 0;
    }


    public void setupMetrics() throws IOException {
        String metricsFileName = getClass().getSimpleName() + "-metrics";
        File metricsFile = Util.getOutputFileRollOldOne(metricsFileName);

        MetricsPlugin metricsPlugin = new HBaseMetricsPlugin(getHBaseConf(), hbaseMetrics);

        metrics = new Metrics(metricsFile, metricsPlugin);

        System.out.println("Metrics are written to " + metricsFileName);
    }

    public void startExecutor() {
        System.out.println("Tasks will run on " + workers + " threads");

        executor = new ThreadPoolExecutor(workers, workers, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
        executor.setRejectedExecutionHandler(new WaitPolicy());
    }

    public void stopExecutor() throws InterruptedException {
        executor.shutdown();
        boolean successfulFinish = executor.awaitTermination(5, TimeUnit.MINUTES);
        if (!successfulFinish) {
            System.out.println("Executor did not end successfully");
        }
        executor = null;
    }

    public Configuration getHBaseConf() {
        Configuration hbaseConf = HBaseConfiguration.create();

        // TODO
        if (zkConnectionString.contains(":")) {
            System.err.println("ATTENTION: do not include port numbers in zookeeper connection string when using features/tests that use HBase.");
        }

        hbaseConf.set("hbase.zookeeper.quorum", zkConnectionString);

        return hbaseConf;
    }
}
