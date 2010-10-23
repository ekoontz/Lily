package org.lilyproject.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexGeneralState;
import org.lilyproject.util.ObjectUtils;

import java.util.Arrays;
import java.util.List;

public class UpdateIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-update-index";
    }

    public static void main(String[] args) {
        new UpdateIndexCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        nameOption.setRequired(true);

        options.add(nameOption);
        options.add(solrShardsOption);
        options.add(shardingConfigurationOption);
        options.add(configurationOption);
        options.add(generalStateOption);
        options.add(updateStateOption);
        options.add(buildStateOption);
        options.add(forceOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

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
            // In case we requested deletion of an index, it might be that the lock is already removed
            // by the time we get here as part of the index deletion.
            boolean ignoreMissing = generalState != null && generalState == IndexGeneralState.DELETE_REQUESTED;
            model.unlockIndex(lock, ignoreMissing);
        }

        return 0;
    }
}
