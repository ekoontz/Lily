package org.lilycms.indexer.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.indexer.model.api.*;
import org.lilycms.util.zookeeper.LeaderElection;
import org.lilycms.util.zookeeper.LeaderElectionCallback;
import org.lilycms.util.zookeeper.LeaderElectionSetupException;
import org.lilycms.util.zookeeper.ZooKeeperItf;
import static org.lilycms.indexer.model.api.IndexerModelEventType.*;

import javax.annotation.PostConstruct;
import java.util.Collection;

public class IndexerMaster {
    private ZooKeeperItf zk;

    private WriteableIndexerModel indexerModel;

    private IndexerModelListener listener = new MyListener();

    private final Object lock = new Object();

    private int currentMaxConsumerId;

    private final Log log = LogFactory.getLog(getClass());

    public IndexerMaster(ZooKeeperItf zk , WriteableIndexerModel indexerModel) {
        this.zk = zk;
        this.indexerModel = indexerModel;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException {
        LeaderElection leaderElection = new LeaderElection(zk, "Indexer Master", "/lily/indexer/masters",
                new MyLeaderElectionCallback());
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        public void elected() {
            log.info("I am elected as the IndexerMaster.");
            initAsMaster();
        }

        public void noLongerElected() {
            log.info("I am no longer the IndexerMaster.");
        }
    }

    private void initAsMaster() {
        initMaxConsumerId();

        synchronized (lock) {
            Collection<IndexDefinition> indexes = indexerModel.getIndexes(listener);

            for (IndexDefinition index : indexes) {
                if (needsConsumerIdAssigned(index)) {
                    assignConsumer(index.getName());
                }
            }
        }
    }

    private boolean needsConsumerIdAssigned(IndexDefinition index) {
        return index.getState() == IndexState.READY && index.getMessageConsumerId() == null;
    }

    /**
     * TODO This is temporary code to find out the currently highest assigned message consumer ID.
     * It should be adapated once a real system for (un)registering MQ consumers is in place.
     */
    private void initMaxConsumerId() {
        int currentMaxId = 10000;
        for (IndexDefinition index : indexerModel.getIndexes()) {
            if (index.getMessageConsumerId() != null) {
                int id = Integer.parseInt(index.getMessageConsumerId());
                if (id > currentMaxId) {
                    currentMaxId = id;
                }
            }
        }
        this.currentMaxConsumerId = currentMaxId;
    }


    private void assignConsumer(String indexName) {
        try {
            String lock = indexerModel.lockIndex(indexName);
            try {
                // Read current situation of record and assure it is still actual
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                if (needsConsumerIdAssigned(index)) {
                    String newConsumerId = String.valueOf(++currentMaxConsumerId);
                    index.setMessageConsumerId(newConsumerId);
                    indexerModel.updateIndex(index);
                    log.info("Assigned message consumer ID " + newConsumerId + " to index " + indexName);
                }
            } finally {
                indexerModel.unlockIndex(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to assign a message consumer to index " + indexName, t);
        }
    }

    private class MyListener implements IndexerModelListener {
        public void process(IndexerModelEvent event) {
            if (event.getType() == INDEX_ADDED || event.getType() == INDEX_UPDATED) {
                IndexDefinition index = null;
                try {
                    index = indexerModel.getIndex(event.getIndexName());
                } catch (IndexNotFoundException e) {
                    // ignore, index has meanwhile been deleted, we will get another event for this
                }

                if (index != null) {
                    synchronized (lock) {
                        if (needsConsumerIdAssigned(index)) {
                            assignConsumer(index.getName());
                        }
                    }
                }
            }
        }
    }
}
