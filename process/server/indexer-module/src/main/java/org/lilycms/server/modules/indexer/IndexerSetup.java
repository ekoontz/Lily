package org.lilycms.server.modules.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.lilycms.hbaseindex.IndexManager;
import org.lilycms.hbaseindex.IndexNotFoundException;
import org.lilycms.indexer.IndexUpdater;
import org.lilycms.indexer.Indexer;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;
import org.lilycms.indexer.conf.IndexerConfException;
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.repository.api.Repository;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.util.io.Closer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class IndexerSetup {
    private Repository repository;
    private RowLog messageQueue;
    private IndexUpdater indexUpdater;
    private Configuration hbaseConf;
    private String indexerConfPath;
    private String solrAddress;

    public IndexerSetup(Repository repository, RowLog messageQueue, Configuration hbaseConf, String indexerConfPath,
            String solrAddress) {
        this.repository = repository;
        this.messageQueue = messageQueue;
        this.hbaseConf = hbaseConf;
        this.indexerConfPath = indexerConfPath;
        this.solrAddress = solrAddress;
    }

    @PostConstruct
    public void start() throws IOException, IndexNotFoundException, IndexerConfException {
        IndexerConf indexerConf;
        InputStream is = null;
        try {
            // TODO read via Hadoop filesystem abstraction?
            is = new FileInputStream(indexerConfPath);
            indexerConf = IndexerConfBuilder.build(is, repository);
        } finally {
            Closer.close(is);
        }

        SolrServer solrServer = new CommonsHttpSolrServer(solrAddress);

        Indexer indexer = new Indexer(indexerConf, repository, solrServer);

        IndexManager.createIndexMetaTableIfNotExists(hbaseConf);
        IndexManager indexManager = new IndexManager(hbaseConf);

        LinkIndex.createIndexes(indexManager);
        LinkIndex linkIndex = new LinkIndex(indexManager, repository);

        indexUpdater = new IndexUpdater(indexer, messageQueue, repository, linkIndex);
    }

    @PreDestroy
    public void stop() {
        if (indexUpdater != null) {
            indexUpdater.stop();
        }
    }
}
