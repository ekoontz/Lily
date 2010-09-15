package org.lilycms.indexer.fullbuild;

import net.iharder.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.lilycms.indexer.Indexer;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;
import org.lilycms.indexer.engine.SolrServers;
import org.lilycms.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.ShardSelector;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexingMapper extends TableMapper<ImmutableBytesWritable, Result> {
    private IdGenerator idGenerator;
    private Indexer indexer;
    private MultiThreadedHttpConnectionManager connectionManager;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", context.getConfiguration().get("hbase.zookeeper.quorum"));
            conf.set("hbase.zookeeper.property.clientPort", context.getConfiguration().get("hbase.zookeeper.property.clientPort"));

            idGenerator = new IdGeneratorImpl();

            TypeManager typeManager = new HBaseTypeManager(idGenerator, conf);

            BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(FileSystem.get(conf));
            BlobStoreAccess defaultBlobStoreAccess = dfsBlobStoreAccess;
            SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(defaultBlobStoreAccess);

            Repository repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, conf);

            byte[] indexerConfBytes = Base64.decode(context.getConfiguration().get("org.lilycms.indexer.fullbuild.indexerconf"));
            IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfBytes), repository);

            Map<String, String> solrShards = new HashMap<String, String>();
            for (int i = 1; true; i++) {
                String shardName = context.getConfiguration().get("org.lilycms.indexer.fullbuild.solrshard.name." + i);
                String shardAddress = context.getConfiguration().get("org.lilycms.indexer.fullbuild.solrshard.address." + i);
                if (shardName == null)
                    break;
                solrShards.put(shardName, shardAddress);
            }

            ShardSelector shardSelector;
            String shardingConf = context.getConfiguration().get("org.lilycms.indexer.fullbuild.shardingconf");
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
        } catch (Exception e) {
            throw new IOException("Error in index build map task setup.", e);
        }
    }

    // TODO shutdown & cleanup

    public void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        RecordId recordId = idGenerator.fromBytes(key.get());

        try {
            indexer.index(recordId);
        } catch (Exception e) {
            throw new IOException("Error indexing record " + recordId, e);
        }
    }
}
