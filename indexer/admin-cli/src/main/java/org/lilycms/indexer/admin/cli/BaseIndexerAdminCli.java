package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.lilycms.cli.BaseZkCliTool;
import org.lilycms.client.LilyClient;
import org.lilycms.indexer.model.api.*;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilycms.indexer.model.indexerconf.IndexerConfException;
import org.lilycms.indexer.model.sharding.ShardingConfigException;
import org.lilycms.util.zookeeper.ZooKeeperImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public abstract class BaseIndexerAdminCli extends BaseZkCliTool {

    protected Option forceOption;
    protected Option nameOption;
    protected Option solrShardsOption;
    protected Option shardingConfigurationOption;
    protected Option configurationOption;
    protected Option generalStateOption;
    protected Option updateStateOption;
    protected Option buildStateOption;

    protected String indexName;
    protected Map<String, String> solrShards;
    protected IndexGeneralState generalState;
    protected IndexUpdateState updateState;
    protected IndexBatchBuildState buildState;
    protected byte[] indexerConfiguration;
    protected byte[] shardingConfiguration;
    protected WriteableIndexerModel model;

    public BaseIndexerAdminCli() {
        // Here we instantiate various options, but it is up to subclasses to decide which ones
        // they acutally want to use (see getOptions() method).
        forceOption = OptionBuilder
                .withDescription("Skips optional validations.")
                .withLongOpt("force")
                .create("f");

        nameOption = OptionBuilder
                .withArgName("name")
                .hasArg()
                .withDescription("Index name.")
                .withLongOpt("name")
                .create("n");

        solrShardsOption = OptionBuilder
                .withArgName("solr-shards")
                .hasArg()
                .withDescription("Comma separated list of 'shardname:URL' pairs pointing to SOLR instances.")
                .withLongOpt("solr-shards")
                .create("s");

        shardingConfigurationOption = OptionBuilder
                .withArgName("shardingconfig.json")
                .hasArg()
                .withDescription("Sharding configuration.")
                .withLongOpt("sharding-config")
                .create("p");

        configurationOption = OptionBuilder
                .withArgName("indexerconfig.xml")
                .hasArg()
                .withDescription("Indexer configuration.")
                .withLongOpt("indexer-config")
                .create("c");

        generalStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("General state, one of: " + getStates(IndexGeneralState.values()))
                .withLongOpt("state")
                .create("i");

        updateStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("Update state, one of: " + getStates(IndexUpdateState.values()))
                .withLongOpt("update-state")
                .create("u");

        buildStateOption = OptionBuilder
                .withArgName("state")
                .hasArg()
                .withDescription("Build state, only: " + IndexBatchBuildState.BUILD_REQUESTED)
                .withLongOpt("build-state")
                .create("b");
    }

    @Override
    protected void reportThrowable(Throwable throwable) {
        if (throwable instanceof IndexValidityException) {
            System.out.println("ATTENTION");
            System.out.println("---------");
            System.out.println("The index could not be created or updated because:");
            printExceptionMessages(throwable);
        } else {
            super.reportThrowable(throwable);
        }
    }

    protected static void printExceptionMessages(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            String prefix = "";
            if (cause instanceof IndexValidityException) {
                prefix = "Index definition: ";
            } else if (cause instanceof IndexerConfException) {
                prefix = "Indexer configuration: ";
            } else if (cause instanceof ShardingConfigException) {
                prefix = "Sharding configuration: ";
            }
            System.out.println(prefix + cause.getMessage());
            cause = cause.getCause();
        }
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        options.add(forceOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0)
            return result;

        if (cmd.hasOption(nameOption.getOpt())) {
            indexName = cmd.getOptionValue(nameOption.getOpt());
        }

        if (cmd.hasOption(solrShardsOption.getOpt())) {
            solrShards = new HashMap<String, String>();
            String[] solrShardEntries = cmd.getOptionValue(solrShardsOption.getOpt()).split(",");
            Set<String> addresses = new HashSet<String>();
            // Be helpful to the user and validate the URIs are syntactically correct
            for (String shardEntry : solrShardEntries) {
                int sep = shardEntry.indexOf(':');
                if (sep == -1) {
                    System.out.println("SOLR shards should be specified as 'name:URL' pairs, which the following is not:");
                    System.out.println(shardEntry);
                    return 1;
                }

                String shardName = shardEntry.substring(0, sep).trim();
                if (shardName.length() == 0) {
                    System.out.println("Zero-length shard name in the following shard entry:");
                    System.out.println(shardEntry);
                    return 1;
                }

                String shardAddress = shardEntry.substring(sep + 1).trim();
                try {
                    URI uri = new URI(shardAddress);
                    if (!uri.isAbsolute()) {
                        System.out.println("Not an absolute URI: " + shardAddress);
                        return 1;
                    }
                } catch (URISyntaxException e) {
                    System.out.println("Invalid SOLR shard URI: " + shardAddress);
                    System.out.println(e.getMessage());
                    return 1;
                }

                if (solrShards.containsKey(shardName)) {
                    System.out.println("Duplicate shard name: " + shardName);
                    return 1;
                }

                if (addresses.contains(shardAddress)) {
                    System.out.println("!!!");
                    System.out.println("Warning: you have two shards pointing to the same URI:");
                    System.out.println(shardAddress);
                }

                addresses.add(shardAddress);

                solrShards.put(shardName, shardAddress);
            }

            if (solrShards.isEmpty()) {
                // Probably cannot occur
                System.out.println("No SOLR shards specified though option is used.");
                return 1;
            }
        }

        if (cmd.hasOption(shardingConfigurationOption.getOpt())) {
            File configurationFile = new File(cmd.getOptionValue(shardingConfigurationOption.getOpt()));

            if (!configurationFile.exists()) {
                System.out.println("Specified sharding configuration file not found:");
                System.out.println(configurationFile.getAbsolutePath());
                return 1;
            }

            shardingConfiguration = FileUtils.readFileToByteArray(configurationFile);
        }

        if (cmd.hasOption(configurationOption.getOpt())) {
            File configurationFile = new File(cmd.getOptionValue(configurationOption.getOpt()));

            if (!configurationFile.exists()) {
                System.out.println("Specified indexer configuration file not found:");
                System.out.println(configurationFile.getAbsolutePath());
                return 1;
            }

            indexerConfiguration = FileUtils.readFileToByteArray(configurationFile);

            if (!cmd.hasOption(forceOption.getOpt())) {
                try {
                    LilyClient lilyClient = new LilyClient(zkConnectionString);
                    IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfiguration), lilyClient.getRepository());
                } catch (Exception e) {
                    System.out.println(); // separator line because LilyClient might have produced some error logs
                    System.out.println("Failed to parse & build the indexer configuration.");
                    System.out.println();
                    System.out.println("If this problem occurs because no Lily node is available");
                    System.out.println("or because certain field types or record types do not exist,");
                    System.out.println("then you can skip this validation using the option --" + forceOption.getLongOpt());
                    System.out.println();
                    if (e instanceof IndexerConfException) {
                        printExceptionMessages(e);
                    } else {
                        e.printStackTrace();
                    }
                    return 1;
                }
            }
        }

        if (cmd.hasOption(generalStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(generalStateOption.getOpt());
            try {
                generalState = IndexGeneralState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index state: " + stateName);
                return 1;
            }
        }

        if (cmd.hasOption(updateStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(updateStateOption.getOpt());
            try {
                updateState = IndexUpdateState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index update state: " + stateName);
                return 1;
            }
        }

        if (cmd.hasOption(buildStateOption.getOpt())) {
            String stateName = cmd.getOptionValue(buildStateOption.getOpt());
            try {
                buildState = IndexBatchBuildState.valueOf(stateName.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid index build state: " + stateName);
                return 1;
            }

            if (buildState != IndexBatchBuildState.BUILD_REQUESTED) {
                System.out.println("The build state can only be set to " + IndexBatchBuildState.BUILD_REQUESTED);
                return 1;
            }
        }

        return 0;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        ZooKeeperItf zk = new ZooKeeperImpl(zkConnectionString, 3000, new MyWatcher());
        model = new IndexerModelImpl(zk);

        return 0;
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

    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }
}
