package org.lilycms.indexer.fullbuild;

import net.iharder.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.lilycms.indexer.Indexer;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class IndexingMapper extends TableMapper<ImmutableBytesWritable, Result> {
    private IdGenerator idGenerator;
    private Indexer indexer;

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
            // TODO sharding
            String solrShard = context.getConfiguration().get("org.lilycms.indexer.fullbuild.solrshard.1");

            IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfBytes), repository);
            SolrServer solrServer = new CommonsHttpSolrServer(solrShard);
            indexer = new Indexer(indexerConf, repository, solrServer);
        } catch (Exception e) {
            throw new IOException("Error in index build map task setup.", e);
        }
    }

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
