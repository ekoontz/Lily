package org.lilyproject.hbaseindex.perftest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.lilyproject.hbaseindex.*;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.testclientfw.BaseTestTool;
import org.lilyproject.testclientfw.Util;
import org.lilyproject.testclientfw.Words;

import java.util.ArrayList;
import java.util.List;


public class HbaseIndexPerfTest extends BaseTestTool {
    private Index index;

    private IdGenerator idGenerator = new IdGeneratorImpl();

    private Option initialInsertOption;
    private Option initialInsertBatchOption;
    private Option loopsOption;

    private int initialInserts;
    private int initialInsertsBatchSize;
    private int loops;

    private int maxResults = 100;

    public static void main(String[] args) throws Exception {
        new HbaseIndexPerfTest().start(args);
    }

    @Override
    protected String getCmdName() {
        return "hbaseindex-perftest";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        initialInsertOption = OptionBuilder
                .withArgName("amount")
                .hasArg()
                .withDescription("Initial index loading: number of entries to create")
                .withLongOpt("initial-entries")
                .create("e");
        options.add(initialInsertOption);

        initialInsertBatchOption = OptionBuilder
                .withArgName("amount")
                .hasArg()
                .withDescription("Initial index loading: number of entries to add in one call to the index")
                .withLongOpt("initial-entries-batch")
                .create("b");
        options.add(initialInsertBatchOption);

        loopsOption = OptionBuilder
                .withArgName("amount")
                .hasArg()
                .withDescription("Number of loops to perform (each loop does multiple operations)")
                .withLongOpt("loops")
                .create("l");
        options.add(loopsOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        Configuration hbaseConf = getHBaseConf();

        IndexManager.createIndexMetaTableIfNotExists(hbaseConf);

        IndexManager indexMgr = new IndexManager(hbaseConf);

        String indexName = "perftest1";

        IndexDefinition indexDef = new IndexDefinition(indexName);
        indexDef.addStringField("word");
        indexDef.addLongField("number");

        indexMgr.createIndexIfNotExists(indexDef);

        initialInserts = Util.getIntOption(cmd, initialInsertOption, 5000000);
        initialInsertsBatchSize = Util.getIntOption(cmd, initialInsertBatchOption, 300);
        loops = Util.getIntOption(cmd, loopsOption, 100000);

        System.out.println("Will insert " + initialInserts + " index entries in batches of " + initialInsertBatchOption);
        System.out.println("Will then perform " + loops + " tests on it");

        index = indexMgr.getIndex(indexName);

        setupMetrics();

        doBulkLoad();

        doUsage();

        finishMetrics();

        return 0;
    }

    private void doBulkLoad() throws InterruptedException {
        startExecutor();

        int left = initialInserts;

        while (left > 0) {
            int amount = Math.min(left, initialInsertsBatchSize);
            left -= amount;
            executor.submit(new BulkInserter(amount));
        }

        stopExecutor();
    }

    private void doUsage() throws InterruptedException {
        startExecutor();


        for (int i = 0; i < loops; i++) {
            executor.submit(new SingleFieldEqualsQuery());
            executor.submit(new StringRangeQuery());
            //executor.submit(new BulkInserter(10));
        }

        stopExecutor();
    }

    private class BulkInserter implements Runnable {
        private int amount;

        public BulkInserter(int amount) {
            this.amount = amount;
        }

        public void run() {
            try {
                List<IndexEntry> entries = new ArrayList<IndexEntry>(amount);

                for (int i = 0; i < amount; i++) {
                    IndexEntry entry = new IndexEntry();
                    entry.addField("word", Words.get());
                    entry.addField("number", (long)Math.floor(Math.random() * Long.MAX_VALUE));
                    entry.setIdentifier(idGenerator.newRecordId().toBytes());
                    entries.add(entry);
                }

                long before = System.nanoTime();
                index.addEntries(entries);
                double duration = System.nanoTime() - before;
                metrics.increment("Index insert in batch of " + amount, "I", amount, duration / 1e6d);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private class SingleFieldEqualsQuery implements Runnable {
        public void run() {
            try {
                Query query = new Query();
                query.addEqualsCondition("word", Words.get());

                int resultCount = 0;

                long before = System.nanoTime();
                QueryResult result = index.performQuery(query);
                while (result.next() != null && resultCount < maxResults) {
                    resultCount++;
                }
                double duration = System.nanoTime() - before;
                metrics.increment("Single field query duration", "Q", duration / 1e6d);
                metrics.increment("Single field query # of results", resultCount);
                result.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private class StringRangeQuery implements Runnable {
        public void run() {
            try {
                String word = "";
                while (word.length() < 3)
                    word = Words.get();

                String prefix = word.substring(0, 3);

                Query query = new Query();
                query.setRangeCondition("word", prefix, prefix, true, true);

                int resultCount = 0;

                long before = System.nanoTime();
                QueryResult result = index.performQuery(query);
                while (result.next() != null && resultCount < maxResults) {
                    resultCount++;
                }
                double duration = System.nanoTime() - before;
                metrics.increment("Str rng query duration", "Q", duration / 1e6d);
                metrics.increment("Str rng query # of results", resultCount);
                result.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}
