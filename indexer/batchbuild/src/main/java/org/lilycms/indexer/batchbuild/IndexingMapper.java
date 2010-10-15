package org.lilycms.indexer.batchbuild;

import net.iharder.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.solr.client.solrj.SolrServerException;
import org.lilycms.client.LilyClient;
import org.lilycms.indexer.engine.IndexLocker;
import org.lilycms.indexer.engine.Indexer;
import org.lilycms.indexer.model.indexerconf.IndexerConf;
import org.lilycms.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilycms.indexer.engine.SolrServers;
import org.lilycms.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.ShardSelector;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class IndexingMapper extends TableMapper<ImmutableBytesWritable, Result> {
    private IdGenerator idGenerator;
    private Indexer indexer;
    private MultiThreadedHttpConnectionManager connectionManager;
    private IndexLocker indexLocker;
    private ZooKeeperItf zk;
    private Repository repository;
    private ThreadPoolExecutor executor;
    private Log log = LogFactory.getLog(getClass());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            Configuration jobConf = context.getConfiguration();

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", jobConf.get("hbase.zookeeper.quorum"));
            conf.set("hbase.zookeeper.property.clientPort", jobConf.get("hbase.zookeeper.property.clientPort"));

            idGenerator = new IdGeneratorImpl();

            String zkConnectString = jobConf.get("org.lilycms.indexer.batchbuild.zooKeeperConnectString");
            int zkSessionTimeout = getIntProp("org.lilycms.indexer.batchbuild.zooKeeperSessionTimeout", null, jobConf);
            zk = ZkUtil.connect(zkConnectString, zkSessionTimeout);

            TypeManager typeManager = new HBaseTypeManager(idGenerator, conf, zk);

            BlobStoreAccessFactory blobStoreAccessFactory = LilyClient.getBlobStoreAccess(zk);

            RowLog wal = new DummyRowLog("The write ahead log should not be called from within MapReduce jobs.");
            repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, wal, conf);

            byte[] indexerConfBytes = Base64.decode(jobConf.get("org.lilycms.indexer.batchbuild.indexerconf"));
            IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfBytes), repository);

            Map<String, String> solrShards = new HashMap<String, String>();
            for (int i = 1; true; i++) {
                String shardName = jobConf.get("org.lilycms.indexer.batchbuild.solrshard.name." + i);
                String shardAddress = jobConf.get("org.lilycms.indexer.batchbuild.solrshard.address." + i);
                if (shardName == null)
                    break;
                solrShards.put(shardName, shardAddress);
            }

            ShardSelector shardSelector;
            String shardingConf = jobConf.get("org.lilycms.indexer.batchbuild.shardingconf");
            if (shardingConf != null) {
                byte[] shardingConfBytes = Base64.decode(shardingConf);
                shardSelector = JsonShardSelectorBuilder.build(shardingConfBytes);
            } else {
                shardSelector = DefaultShardSelectorBuilder.createDefaultSelector(solrShards);
            }

            connectionManager = new MultiThreadedHttpConnectionManager();
            connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
            connectionManager.getParams().setMaxTotalConnections(50);
            HttpClient httpClient = new HttpClient(connectionManager);

            SolrServers solrServers = new SolrServers(solrShards, shardSelector, httpClient);

            indexLocker = new IndexLocker(zk);

            indexer = new Indexer(indexerConf, repository, solrServers, indexLocker);

            int workers = getIntProp("org.lilycms.indexer.batchbuild.threads", 5, jobConf);
            
            executor = new ThreadPoolExecutor(workers, workers, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        } catch (Exception e) {
            throw new IOException("Error in index build map task setup.", e);
        }
    }

    private int getIntProp(String name, Integer defaultValue, Configuration conf) {
        String value = conf.get(name);
        if (value == null) {
            if (defaultValue != null)
                return defaultValue;
            else
                throw new RuntimeException("Missing property in jobconf: " + name);
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer value in jobconf property. Property '" + name + "', value: " +
                    value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        executor.shutdown();
        boolean successfulFinish = executor.awaitTermination(5, TimeUnit.MINUTES);
        if (!successfulFinish) {
            log.error("Executor did not finish outstanding work within the foreseen timeout.");
        }

        Closer.close(connectionManager);
        Closer.close(repository);
        super.cleanup(context);
        Closer.close(zk);
    }

    public void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        executor.submit(new MappingTask(context.getCurrentKey().get(), context));
    }

    public class MappingTask implements Runnable {
        private byte[] key;
        private Context context;

        private MappingTask(byte[] key, Context context) {
            this.key = key;
            this.context = context;
        }

        public void run() {
            RecordId recordId = null;
            boolean locked = false;
            try {
                recordId = idGenerator.fromBytes(key);
                indexLocker.lock(recordId);
                locked = true;
                indexer.index(recordId);
            } catch (Throwable t) {
                context.getCounter(IndexBatchBuildCounters.NUM_FAILED_RECORDS).increment(1);

                // Avoid printing a complete stack trace for common errors.
                if (t instanceof SolrServerException && t.getMessage().equals("java.net.ConnectException: Connection refused")) {
                    log.error("Failure indexing record " + recordId + ": SOLR connection refused.");
                } else {
                    log.error("Failure indexing record " + recordId, t);
                }

            } finally {
                if (locked) {
                    indexLocker.unlockLogFailure(recordId);
                }
            }
        }
    }

}
