package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.joda.time.DateTime;
import org.lilycms.indexer.model.api.*;
import org.lilycms.indexer.model.impl.IndexerModelImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListIndexesCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-list-indexes";
    }

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

            ActiveBuildJobInfo activeBuildJob = index.getActiveBuildJobInfo();
            if (activeBuildJob != null) {
                System.out.println("  + Active build job:");
                System.out.println("    + Hadoop Job ID: " + activeBuildJob.getJobId());
                System.out.println("    + Submitted at: " + new DateTime(activeBuildJob.getSubmitTime()).toString());
            }

            BuildJobInfo lastBuildJob = index.getLastBuildJobInfo();
            if (lastBuildJob != null) {
                System.out.println("  + Last build job:");
                System.out.println("    + Hadoop Job ID: " + lastBuildJob.getJobId());
                System.out.println("    + Submitted at: " + new DateTime(lastBuildJob.getSubmitTime()).toString());
                System.out.println("    + Success: " + lastBuildJob.getSuccess());
                System.out.println("    + Job state: " + lastBuildJob.getJobState());
            }

            System.out.println();
        }
    }
}