package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.WriteableIndexerModel;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
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
        options.add(configurationOption);
        options.add(stateOption);

        return options;
    }

    public void run(ZooKeeperItf zk, CommandLine cmd) throws Exception {
        WriteableIndexerModel model = new IndexerModelImpl(zk);

        if (!model.hasIndex(indexName)) {
            System.out.println("Index does not exist: " + indexName);
            System.exit(1);
        }

        String lock = model.lockIndex(indexName);
        try {
            IndexDefinition index = model.getMutableIndex(indexName);

            boolean changes = false;

            if (solrShards != null && !solrShards.equals(index.getSolrShards())) {
                index.setSolrShards(solrShards);
                changes = true;
            }

            if (indexerConfiguration != null && !Arrays.equals(indexerConfiguration, index.getConfiguration())) {
                index.setConfiguration(indexerConfiguration);
                changes = true;
            }

            if (stateOption != null && indexState != index.getState()) {
                index.setState(indexState);
                changes = true;
            }

            if (changes) {
                model.updateIndex(index);
                System.out.println("Index updated: " + indexName);
            } else {
                System.out.println("Index already matches the specified settings, did not update it.");
            }


        } finally {
            model.unlockIndex(lock);
        }
    }
}
