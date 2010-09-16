package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.*;
import org.lilycms.indexer.model.api.IndexDefinition;

import java.util.List;

public class AddIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-add-index";
    }

    public static void main(String[] args) {
        new AddIndexCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        nameOption.setRequired(true);
        solrShardsOption.setRequired(true);
        configurationOption.setRequired(true);

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

        IndexDefinition index = model.newIndex(indexName);

        index.setSolrShards(solrShards);

        index.setConfiguration(indexerConfiguration);

        if (shardingConfiguration != null)
            index.setShardingConfiguration(shardingConfiguration);

        if (generalState != null)
            index.setGeneralState(generalState);

        if (updateState != null)
            index.setUpdateState(updateState);

        if (buildState != null)
            index.setBatchBuildState(buildState);

        model.addIndex(index);

        System.out.println("Index created: " + indexName);

        return 0;
    }

}
