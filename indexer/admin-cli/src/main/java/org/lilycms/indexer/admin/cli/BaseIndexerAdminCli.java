package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.lilycms.indexer.model.api.IndexBatchBuildState;
import org.lilycms.indexer.model.api.IndexGeneralState;
import org.lilycms.indexer.model.api.IndexUpdateState;
import org.lilycms.util.zookeeper.ZooKeeperImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public abstract class BaseIndexerAdminCli {
    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";

    protected Option nameOption;
    protected Option solrShardsOption;
    protected Option configurationOption;
    protected Option generalStateOption;
    protected Option updateStateOption;
    protected Option buildStateOption;

    protected String zkConnectionString;
    protected String indexName;
    protected List<String> solrShards;
    protected IndexGeneralState generalState;
    protected IndexUpdateState updateState;
    protected IndexBatchBuildState buildState;
    protected byte[] indexerConfiguration;

    public static void start(String[] args, BaseIndexerAdminCli cli) {
        try {
            cli.runBase(args);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void runBase(String[] args) throws Exception {
        Options cliOptions = new Options();

        //
        // Fixed options
        //
        Option helpOption = new Option("h", "help", false, "Shows help");
        cliOptions.addOption(helpOption);

        Option zkOption = OptionBuilder
                .withArgName("connection-string")
                .hasArg()
                .withDescription("Zookeeper connection string: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");
        cliOptions.addOption(zkOption);

        //
        // Instantiate default options, but don't add them to the options, it is up to subclasses to reuse
        // some of these.
        //
        nameOption = OptionBuilder
                .withArgName("name")
                .hasArg()
                .withDescription("Index name.")
                .withLongOpt("name")
                .create("n");
        cliOptions.addOption(nameOption);

        solrShardsOption = OptionBuilder
                .withArgName("solr-urls")
                .hasArg()
                .withDescription("Comma separated list of URLs to SOLR instances (shards).")
                .withLongOpt("solr-shards")
                .withValueSeparator(',')
                .create("s");
        cliOptions.addOption(solrShardsOption);

        configurationOption = OptionBuilder
                .withArgName("indexerconfig.xml")
                .hasArg()
                .withDescription("Indexer configuration.")
                .withLongOpt("indexer-config")
                .create("c");
        cliOptions.addOption(configurationOption);

        generalStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("General state, one of: " + getStates(IndexGeneralState.values()))
                .withLongOpt("state")
                .create("i");
        cliOptions.addOption(generalStateOption);

        updateStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("Update state, one of: " + getStates(IndexUpdateState.values()))
                .withLongOpt("update-state")
                .create("u");
        cliOptions.addOption(updateStateOption);

        buildStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("Build state, only: " + IndexBatchBuildState.BUILD_REQUESTED)
                .withLongOpt("build-state")
                .create("b");
        cliOptions.addOption(buildStateOption);

        //
        // Options of this specific CLI tool
        //
        for (Option option : getOptions()) {
            cliOptions.addOption(option);
        }

        //
        // Parse options
        //
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        boolean showHelp = false;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            showHelp = true;
        }

        //
        // Process fixed options
        //
        if (showHelp || cmd.hasOption(helpOption.getOpt())) {
            printHelp(cliOptions);
            System.exit(1);
        }

        if (!cmd.hasOption(zkOption.getOpt())) {
            System.out.println("Zookeeper connection string not specified, using default: " + DEFAULT_ZK_CONNECT);
            zkConnectionString = DEFAULT_ZK_CONNECT;
        } else {
            zkConnectionString = cmd.getOptionValue(zkOption.getOpt());
        }

        //
        // Process values for common options, if present
        //
        if (cmd.hasOption(nameOption.getOpt())) {
            indexName = cmd.getOptionValue(nameOption.getOpt());
        }

        if (cmd.hasOption(solrShardsOption.getOpt())) {
            solrShards = Arrays.asList(cmd.getOptionValues(solrShardsOption.getOpt()));
            // Be helpful to the user and validate the URIs are syntactically correct
            for (String shard : solrShards) {
                try {
                    URI uri = new URI(shard);
                    if (!uri.isAbsolute()) {
                        System.out.println("Not an absolute URI: " + shard);
                        System.exit(1);
                    }
                } catch (URISyntaxException e) {
                    System.out.println("Invalid SOLR shard URI: " + shard);
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
            }
        }

        if (cmd.hasOption(configurationOption.getOpt())) {
            File configurationFile = new File(cmd.getOptionValue(configurationOption.getOpt()));

            if (!configurationFile.exists()) {
                System.out.println("Specified indexer configuration file not found:");
                System.out.println(configurationFile.getAbsolutePath());
                System.exit(1);
            }

            indexerConfiguration = FileUtils.readFileToByteArray(configurationFile);
        }

        if (cmd.hasOption(generalStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(generalStateOption.getOpt());
            try {
                generalState = IndexGeneralState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index state: " + stateName);
                System.exit(1);
            }
        }

        if (cmd.hasOption(updateStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(updateStateOption.getOpt());
            try {
                updateState = IndexUpdateState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index update state: " + stateName);
                System.exit(1);
            }
        }

        if (cmd.hasOption(buildStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(buildStateOption.getOpt());
            try {
                buildState = IndexBatchBuildState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index build state: " + stateName);
                System.exit(1);
            }

            if (buildState != IndexBatchBuildState.BUILD_REQUESTED) {
                System.out.println("The build state can only be set to " + IndexBatchBuildState.BUILD_REQUESTED);
                System.exit(1);
            }
        }

        ZooKeeperItf zk = new ZooKeeperImpl(zkConnectionString, 3000, new MyWatcher());

        run(zk, cmd);
    }

    private String getStates(Enum[] values) {
        StringBuilder builder = new StringBuilder();
        for (Enum value : values) {
            if (builder.length() > 0)
                builder.append(", ");
            builder.append(value);
        }
        return builder.toString();
    }

    public abstract List<Option> getOptions();

    public abstract void run(ZooKeeperItf zk, CommandLine cmd) throws Exception;

    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }

    protected abstract String getCmdName();

    private void printHelp(Options cliOptions) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp(getCmdName(), cliOptions, true);
    }
}
