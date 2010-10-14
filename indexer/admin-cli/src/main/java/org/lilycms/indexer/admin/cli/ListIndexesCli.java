package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.joda.time.DateTime;
import org.lilycms.indexer.model.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ListIndexesCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-list-indexes";
    }

    public static void main(String[] args) {
        new ListIndexesCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        return super.getOptions();
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        List<IndexDefinition> indexes = new ArrayList<IndexDefinition>(model.getIndexes());
        Collections.sort(indexes, IndexDefinitionNameComparator.INSTANCE);

        System.out.println("Number of indexes: " + indexes.size());
        System.out.println();

        for (IndexDefinition index : indexes) {
            System.out.println(index.getName());
            System.out.println("  + General state: " + index.getGeneralState());
            System.out.println("  + Update state: " + index.getUpdateState());
            System.out.println("  + Batch build state: " + index.getBatchBuildState());
            System.out.println("  + Queue subscription ID: " + index.getQueueSubscriptionId());
            System.out.println("  + SOLR shards: ");
            for (Map.Entry<String, String> shard : index.getSolrShards().entrySet()) {
                System.out.println("    + " + shard.getKey() + ": " + shard.getValue());
            }

            ActiveBatchBuildInfo activeBatchBuild = index.getActiveBatchBuildInfo();
            if (activeBatchBuild != null) {
                System.out.println("  + Active batch build:");
                System.out.println("    + Hadoop Job ID: " + activeBatchBuild.getJobId());
                System.out.println("    + Submitted at: " + new DateTime(activeBatchBuild.getSubmitTime()).toString());
                System.out.println("    + Tracking URL: " + activeBatchBuild.getTrackingUrl());
            }

            BatchBuildInfo lastBatchBuild = index.getLastBatchBuildInfo();
            if (lastBatchBuild != null) {
                System.out.println("  + Last batch build:");
                System.out.println("    + Hadoop Job ID: " + lastBatchBuild.getJobId());
                System.out.println("    + Submitted at: " + new DateTime(lastBatchBuild.getSubmitTime()).toString());
                System.out.println("    + Success: " + lastBatchBuild.getSuccess());
                System.out.println("    + Job state: " + lastBatchBuild.getJobState());
                System.out.println("    + Tracking URL: " + lastBatchBuild.getTrackingUrl());
                Map<String, Long> counters = lastBatchBuild.getCounters();
                if (counters.containsKey(COUNTER_MAP_INPUT_RECORDS)) {
                    System.out.println("    + Map input records: " + counters.get(COUNTER_MAP_INPUT_RECORDS));
                }
                if (counters.containsKey(COUNTER_TOTAL_LAUNCHED_MAPS)) {
                    System.out.println("    + Launched map tasks: " + counters.get(COUNTER_TOTAL_LAUNCHED_MAPS));                    
                }
                if (counters.containsKey(COUNTER_NUM_FAILED_MAPS)) {
                    System.out.println("    + Failed map tasks: " + counters.get(COUNTER_NUM_FAILED_MAPS));
                }
                if (counters.containsKey(COUNTER_NUM_FAILED_RECORDS)) {
                    System.out.println("    + Records failed to index: " + counters.get(COUNTER_NUM_FAILED_RECORDS));
                }
            }
        }

        return 0;
    }

    private static final String COUNTER_MAP_INPUT_RECORDS = "org.apache.hadoop.mapred.Task$Counter:MAP_INPUT_RECORDS";
    private static final String COUNTER_TOTAL_LAUNCHED_MAPS = "org.apache.hadoop.mapred.JobInProgress$Counter:TOTAL_LAUNCHED_MAPS";
    private static final String COUNTER_NUM_FAILED_MAPS = "org.apache.hadoop.mapred.JobInProgress$Counter:NUM_FAILED_MAPS";
    private static final String COUNTER_NUM_FAILED_RECORDS = "org.lilycms.indexer.fullbuild.IndexBatchBuildCounters:NUM_FAILED_RECORDS";
}