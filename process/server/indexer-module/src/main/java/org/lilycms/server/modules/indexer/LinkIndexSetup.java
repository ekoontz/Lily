package org.lilycms.server.modules.indexer;

import org.apache.hadoop.conf.Configuration;
import org.lilycms.hbaseindex.IndexManager;
import org.lilycms.hbaseindex.IndexNotFoundException;
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.linkindex.LinkIndexUpdater;
import org.lilycms.repository.api.Repository;
import org.lilycms.rowlog.api.RowLog;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

public class LinkIndexSetup {
    private Repository repository;
    private RowLog writeAheadLog;
    private Configuration hbaseConf;
    private LinkIndex linkIndex;
    private LinkIndexUpdater linkIndexUpdater;

    public LinkIndexSetup(Repository repository, RowLog writeAheadLog, Configuration hbaseConf) {
        this.repository = repository;
        this.writeAheadLog = writeAheadLog;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws IOException, IndexNotFoundException {
        // First, bring the link index up to date.
        // We always make sure the link index is up to date with the current record state, before updating
        // the index entry of the record. This way, from the moment the index entry is inserted, any
        // denormalized data contained in it will be correctly updated.

        IndexManager.createIndexMetaTableIfNotExists(hbaseConf);
        IndexManager indexManager = new IndexManager(hbaseConf);

        LinkIndex.createIndexes(indexManager);
        linkIndex = new LinkIndex(indexManager, repository);

        this.linkIndexUpdater = new LinkIndexUpdater(repository, linkIndex);
    }

    @PreDestroy
    public void stop() {
        //linkIndexUpdater.stop();
    }

    public LinkIndex getLinkIndex() {
        return linkIndex;
    }
}
