package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.IndexDefinitionNameComparator;
import org.lilycms.indexer.model.api.WriteableIndexerModel;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListIndexesCli extends BaseIndexerAdminCli {
    public static void main(String[] args) {
        start(args, new ListIndexesCli());
    }

    @Override
    public List<Option> getOptions() {
        return Collections.emptyList();
    }

    public void run(ZooKeeperItf zk, CommandLine cmd) throws Exception {
        WriteableIndexerModel model = new IndexerModelImpl(zk);

        List<IndexDefinition> indexes = new ArrayList<IndexDefinition>(model.getIndexes());
        Collections.sort(indexes, IndexDefinitionNameComparator.INSTANCE);

        System.out.println("Number of indexes: " + indexes.size());
        System.out.println();

        for (IndexDefinition index : indexes) {
            System.out.println(index.getName());
            System.out.println("  + State: " + index.getState());
            System.out.println("  + Message consumer: " + index.getMessageConsumerId());
            System.out.println("  + SOLR shards: ");
            for (String shard : index.getSolrShards()) {
                System.out.println("    + " + shard);
            }
            System.out.println();
        }
    }


}