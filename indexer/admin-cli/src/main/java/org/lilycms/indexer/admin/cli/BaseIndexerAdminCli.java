package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.lilycms.cli.BaseZkCliTool;
import org.lilycms.client.LilyClient;
import org.lilycms.indexer.model.api.*;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilycms.indexer.model.indexerconf.IndexerConfException;
import org.lilycms.indexer.model.sharding.ShardingConfigException;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.StateWatchingZooKeeper;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;
import org.lilycms.util.zookeeper.ZooKeeperOperation;

import java.io.*;
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
    protected Option outputFileOption;

    protected String indexName;
    protected Map<String, String> solrShards;
    protected IndexGeneralState generalState;
    protected IndexUpdateState updateState;
    protected IndexBatchBuildState buildState;
    protected byte[] indexerConfiguration;
    protected byte[] shardingConfiguration;
    protected WriteableIndexerModel model;
    protected String outputFileName;

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
                .withDescription("General state, one of: " + IndexGeneralState.ACTIVE + ", " +
                        IndexGeneralState.DISABLED + ", " + IndexGeneralState.DELETE_REQUESTED)
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

        outputFileOption = OptionBuilder
                .withArgName("filename")
                .hasArg()
                .withDescription("Output file name")
                .withLongOpt("output-file")
                .create("o");
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

                if (shardName.equals("http")) {
                    System.out.println("You forgot to specify a shard name for the SOLR shard " + shardEntry);
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
                    if (!cmd.hasOption(forceOption.getOpt())) {
                        System.out.println("You have two shards pointing to the same URI:");
                        System.out.println(shardAddress);
                        System.out.println();
                        System.out.println("If this is what you want, use the --" + forceOption.getLongOpt() +
                                " option to bypass this check.");
                        return 1;
                    }
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
                LilyClient lilyClient = null;
                try {
                    lilyClient = new LilyClient(zkConnectionString, 10000);
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
                } finally {
                    Closer.close(lilyClient);
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

        if (cmd.hasOption(outputFileOption.getOpt())) {
            outputFileName = cmd.getOptionValue(outputFileOption.getOpt());
            File file = new File(outputFileName);

            if (!cmd.hasOption(forceOption.getOpt()) && file.exists()) {
                System.out.println("The specified output file already exists:");
                System.out.println(file.getAbsolutePath());
                System.out.println();
                System.out.println("Use --" + forceOption.getLongOpt() + " to overwrite it.");
            }
        }

        return 0;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        final ZooKeeperItf zk = new StateWatchingZooKeeper(zkConnectionString, 10000);

        boolean lilyNodeExists = ZkUtil.retryOperationForever(new ZooKeeperOperation<Boolean>() {
            public Boolean execute() throws KeeperException, InterruptedException {
                return zk.exists("/lily", false) != null;
            }
        });

        if (!lilyNodeExists) {
            if (!cmd.hasOption(forceOption.getOpt())) {
                System.out.println("No /lily node found in ZooKeeper. Are you sure you are connecting to the right");
                System.out.println("ZooKeeper? If so, use the option --" + forceOption.getLongOpt() +
                        " to bypass this check.");
                return 1;
            } else {
                System.out.println("No /lily node found in ZooKeeper. Will continue anyway since you supplied --" +
                        forceOption.getLongOpt());
                System.out.println();
            }
        }

        model = new IndexerModelImpl(zk);

        // Perform some extra validation which we can only do now that we have access
        // to the indexer model: check that any specified SOLR shard URIs are not the
        // same as those of other indexes.
        if (solrShards != null) {
            Collection<IndexDefinition> indexes = model.getIndexes();
            for (String uri : solrShards.values()) {
                for (IndexDefinition index : indexes) {
                    if (indexName != null && index.getName().equals(indexName))
                        continue;

                    for (String uri2 : index.getSolrShards().values()) {
                        if (uri.equals(uri2)) {
                            System.out.println("The following SOLR shard URI:");
                            System.out.println(uri);
                            System.out.println("is already in use by the index " + index.getName());
                            if (!cmd.hasOption(forceOption.getOpt())) {
                                System.out.println("If you are ok with this, use the --" + forceOption.getLongOpt() +
                                        " option to bypass this check.");
                                return 1;
                            } else {
                                System.out.println("Since --" + forceOption.getLongOpt() +
                                        " is specified, this will be ignored.");
                                System.out.println();
                            }
                        }
                    }
                }
            }
        }

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

    protected OutputStream getOutput() throws FileNotFoundException {
        if (outputFileName == null) {
            return System.out;
        } else {
            return new FileOutputStream(outputFileName);
        }
    }
}
