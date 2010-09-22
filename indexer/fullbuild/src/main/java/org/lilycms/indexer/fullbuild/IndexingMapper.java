package org.lilycms.indexer.fullbuild;

import net.iharder.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.lilycms.client.LilyClient;
import org.lilycms.indexer.engine.IndexLock;
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
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.ZooKeeperImpl;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexingMapper extends TableMapper<ImmutableBytesWritable, Result> {
    private IdGenerator idGenerator;
    private Indexer indexer;
    private MultiThreadedHttpConnectionManager connectionManager;
    private IndexLocker indexLocker;
    private ZooKeeperItf zk;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            Configuration jobConf = context.getConfiguration();

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", jobConf.get("hbase.zookeeper.quorum"));
            conf.set("hbase.zookeeper.property.clientPort", jobConf.get("hbase.zookeeper.property.clientPort"));

            idGenerator = new IdGeneratorImpl();

            TypeManager typeManager = new HBaseTypeManager(idGenerator, conf);

            String zkConnectString = jobConf.get("org.lilycms.indexer.fullbuild.zooKeeperConnectString");
            int zkSessionTimeout = Integer.parseInt(jobConf.get("org.lilycms.indexer.fullbuild.zooKeeperSessionTimeout"));
            zk = new ZooKeeperImpl(zkConnectString, zkSessionTimeout, new Watcher() {
                public void process(WatchedEvent event) {
                }
            });

            BlobStoreAccessFactory blobStoreAccessFactory = LilyClient.getBlobStoreAccess(zk);

            Repository repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, conf);

            byte[] indexerConfBytes = Base64.decode(jobConf.get("org.lilycms.indexer.fullbuild.indexerconf"));
            IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfBytes), repository);

            Map<String, String> solrShards = new HashMap<String, String>();
            for (int i = 1; true; i++) {
                String shardName = jobConf.get("org.lilycms.indexer.fullbuild.solrshard.name." + i);
                String shardAddress = jobConf.get("org.lilycms.indexer.fullbuild.solrshard.address." + i);
                if (shardName == null)
                    break;
                solrShards.put(shardName, shardAddress);
            }

            ShardSelector shardSelector;
            String shardingConf = jobConf.get("org.lilycms.indexer.fullbuild.shardingconf");
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

            indexer = new Indexer(indexerConf, repository, solrServers);

            indexLocker = new IndexLocker(zk);
        } catch (Exception e) {
            throw new IOException("Error in index build map task setup.", e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        Closer.close(zk);
    }

    // TODO shutdown & cleanup

    public void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        RecordId recordId = idGenerator.fromBytes(key.get());

        IndexLock indexLock = null;
        try {
            indexLock = indexLocker.lock(recordId);
            indexer.index(recordId, indexLock);
        } catch (Exception e) {
            throw new IOException("Error indexing record " + recordId, e);
        } finally {
            if (indexLock != null)
                indexLocker.unlockLogFailure(recordId, indexLock);
        }
    }
}
