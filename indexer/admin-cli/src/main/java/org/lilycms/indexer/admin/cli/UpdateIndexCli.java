package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.WriteableIndexerModel;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.util.ObjectUtils;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UpdateIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-update-index";
    }

    public static void main(String[] args) {
        start(args, new UpdateIndexCli());
    }

    @Override
    public List<Option> getOptions() {
        nameOption.setRequired(true);

        List<Option> options = new ArrayList<Option>();
        options.add(nameOption);
        options.add(solrShardsOption);
        options.add(shardingConfigurationOption);
        options.add(configurationOption);
        options.add(generalStateOption);
        options.add(updateStateOption);
        options.add(buildStateOption);

        return options;
    }

    public int run(ZooKeeperItf zk, CommandLine cmd) throws Exception {
        WriteableIndexerModel model = new IndexerModelImpl(zk);

        if (!model.hasIndex(indexName)) {
            System.out.println("Index does not exist: " + indexName);
            return 1;
        }

        String lock = model.lockIndex(indexName);
        try {
            IndexDefinition index = model.getMutableIndex(indexName);

            boolean changes = false;

            if (solrShards != null && !solrShards.equals(index.getSolrShards())) {
                index.setSolrShards(solrShards);
                changes = true;
            }

            if (shardingConfiguration != null && !ObjectUtils.safeEquals(shardingConfiguration, index.getShardingConfiguration())) {
                index.setShardingConfiguration(shardingConfiguration);
                changes = true;
            }

            if (indexerConfiguration != null && !Arrays.equals(indexerConfiguration, index.getConfiguration())) {
                index.setConfiguration(indexerConfiguration);
                changes = true;
            }

            if (generalState != null && generalState != index.getGeneralState()) {
                index.setGeneralState(generalState);
                changes = true;
            }

            if (updateState != null && updateState != index.getUpdateState()) {
                index.setUpdateState(updateState);
                changes = true;
            }

            if (buildState != null && buildState != index.getBatchBuildState()) {
                index.setBatchBuildState(buildState);
                changes = true;
            }

            if (changes) {
                model.updateIndex(index, lock);
                System.out.println("Index updated: " + indexName);
            } else {
                System.out.println("Index already matches the specified settings, did not update it.");
            }


        } finally {
            model.unlockIndex(lock);
        }

        return 0;
    }
}
