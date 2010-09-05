package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.WriteableIndexerModel;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.util.ArrayList;
import java.util.List;

public class TouchIndexCli extends BaseIndexerAdminCli {

    public static void main(String[] args) {
        start(args, new TouchIndexCli());
    }

    @Override
    public List<Option> getOptions() {
        nameOption.setRequired(true);

        List<Option> options = new ArrayList<Option>();
        options.add(nameOption);

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
            model.updateIndex(index);

            System.out.println("Index touched.");
        } finally {
            model.unlockIndex(lock);
        }
    }
}

