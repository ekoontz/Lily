package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.*;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.WriteableIndexerModel;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.util.ArrayList;
import java.util.List;

public class AddIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-add-index";
    }

    public static void main(String[] args) {
        start(args, new AddIndexCli());
    }

    @Override
    public List<Option> getOptions() {
        nameOption.setRequired(true);
        solrShardsOption.setRequired(true);
        configurationOption.setRequired(true);

        List<Option> options = new ArrayList<Option>();
        options.add(nameOption);
        options.add(solrShardsOption);
        options.add(configurationOption);
        options.add(generalStateOption);
        options.add(updateStateOption);
        options.add(buildStateOption);

        return options;
    }

    public void run(ZooKeeperItf zk, CommandLine cmd) throws Exception {
        WriteableIndexerModel model = new IndexerModelImpl(zk);

        IndexDefinition index = model.newIndex(indexName);

        index.setSolrShards(solrShards);

        index.setConfiguration(indexerConfiguration);

        if (generalState != null)
            index.setGeneralState(generalState);

        if (updateState != null)
            index.setUpdateState(updateState);

        if (buildState != null)
            index.setBatchBuildState(buildState);

        model.addIndex(index);

        System.out.println("Index created: " + indexName);
    }

}
