package org.lilycms.indexer.worker;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.indexer.IndexUpdater;
import org.lilycms.indexer.Indexer;
import org.lilycms.indexer.conf.IndexerConf;
import org.lilycms.indexer.conf.IndexerConfBuilder;
import org.lilycms.indexer.engine.SolrServers;
import org.lilycms.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.ShardSelector;
import org.lilycms.indexer.model.api.*;
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.repository.api.Repository;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.util.ObjectUtils;

import static org.lilycms.indexer.model.api.IndexerModelEventType.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class IndexerWorker {
    private IndexerModel indexerModel;

    private Repository repository;

    private LinkIndex linkIndex;

    private RowLog rowLog;

    private IndexerModelListener listener = new MyListener();

    private Map<String, IndexUpdaterHandle> indexUpdaters = new HashMap<String, IndexUpdaterHandle>();

    private final Object indexUpdatersLock = new Object();

    private BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

    private Thread eventWorkerThread;

    private HttpClient httpClient;

    private MultiThreadedHttpConnectionManager connectionManager;
    
    private final Log log = LogFactory.getLog(getClass());

    public IndexerWorker(IndexerModel indexerModel, Repository repository, RowLog rowLog, LinkIndex linkIndex) {
        this.indexerModel = indexerModel;
        this.repository = repository;
        this.rowLog = rowLog;
        this.linkIndex = linkIndex;
    }

    @PostConstruct
    public void init() {
        connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
      	httpClient = new HttpClient(connectionManager);

        eventWorkerThread = new Thread(new EventWorker());
        eventWorkerThread.start();

        synchronized (indexUpdatersLock) {
            Collection<IndexDefinition> indexes = indexerModel.getIndexes(listener);

            for (IndexDefinition index : indexes) {
                if (shouldRunIndexUpdater(index)) {
                    addIndexUpdater(index);
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        eventWorkerThread.interrupt();
        try {
            eventWorkerThread.join();
        } catch (InterruptedException e) {
            log.info("Interrupted while joining eventWorkerThread.");
        }

        for (IndexUpdaterHandle handle : indexUpdaters.values()) {
            handle.updater.stop();
        }

        connectionManager.shutdown();
    }

    private void addIndexUpdater(IndexDefinition index) {
        try {
            IndexerConf indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(index.getConfiguration()), repository);

            ShardSelector shardSelector;
            if (index.getShardingConfiguration() == null) {
                shardSelector = DefaultShardSelectorBuilder.createDefaultSelector(index.getSolrShards());
            } else {
                shardSelector = JsonShardSelectorBuilder.build(index.getShardingConfiguration());
            }

            checkShardUsage(index.getName(), index.getSolrShards().keySet(), shardSelector.getShards());

            SolrServers solrServers = new SolrServers(index.getSolrShards(), shardSelector, httpClient);
            Indexer indexer = new Indexer(indexerConf, repository, solrServers);

            int consumerId = Integer.parseInt(index.getQueueSubscriptionId());
            IndexUpdater indexUpdater = new IndexUpdater(indexer, rowLog, consumerId, repository, linkIndex);

            IndexUpdaterHandle handle = new IndexUpdaterHandle(index, indexUpdater);

            indexUpdaters.put(index.getName(), handle);

            log.info("Started index updater for index " + index.getName());
        } catch (Throwable t) {
            log.error("Problem starting index updater for index " + index.getName(), t);
        }
    }

    private void checkShardUsage(String indexName, Set<String> definedShards, Set<String> selectorShards) {
        for (String shard : definedShards) {
            if (!selectorShards.contains(shard)) {
                log.warn("A shard is not used by the shard selector. Index: " + indexName + ", shard: " + shard);
            }
        }
    }

    private void updateIndexUpdater(IndexDefinition index) {
        IndexUpdaterHandle handle = indexUpdaters.get(index.getName());

        if (handle.indexDef.getZkDataVersion() >= index.getZkDataVersion()) {
            return;
        }

        boolean relevantChanges = !Arrays.equals(handle.indexDef.getConfiguration(), index.getConfiguration()) ||
                !handle.indexDef.getSolrShards().equals(index.getSolrShards()) ||
                !ObjectUtils.safeEquals(handle.indexDef.getShardingConfiguration(), index.getShardingConfiguration());

        if (!relevantChanges) {
            return;
        }

        if (removeIndexUpdater(index.getName())) {
            addIndexUpdater(index);
        }
    }

    private boolean removeIndexUpdater(String indexName) {
        IndexUpdaterHandle handle = indexUpdaters.get(indexName);

        if (handle == null) {
            return true;
        }

        try {
            handle.updater.stop();
            indexUpdaters.remove(indexName);
            log.info("Stopped indexer updater for index " + indexName);
            return true;
        } catch (Throwable t) {
            log.fatal("Failed to stop an IndexUpdater that should be stopped.", t);
            return false;
        }
    }

    private class MyListener implements IndexerModelListener {
        public void process(IndexerModelEvent event) {
            try {
                // Because the actions we take in response to events might take some time, we
                // let the events process by another thread, so that other watchers do not
                // have to wait too long.
                eventQueue.put(event);
            } catch (InterruptedException e) {
                log.info("IndexerWorker.IndexerModelListener interrupted.");
            }
        }
    }

    private boolean shouldRunIndexUpdater(IndexDefinition index) {
        return index.getUpdateState() == IndexUpdateState.SUBSCRIBE_AND_LISTEN &&
                index.getQueueSubscriptionId() != null;
    }

    private class IndexUpdaterHandle {
        private IndexDefinition indexDef;
        private IndexUpdater updater;

        public IndexUpdaterHandle(IndexDefinition indexDef, IndexUpdater updater) {
            this.indexDef = indexDef;
            this.updater = updater;
        }
    }

    private class EventWorker implements Runnable {
        public void run() {
            while (true) {
                try {
                    int queueSize = eventQueue.size();
                    if (queueSize >= 10) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }

                    IndexerModelEvent event = eventQueue.take();
                    if (event.getType() == INDEX_ADDED || event.getType() == INDEX_UPDATED) {
                        try {
                            IndexDefinition index = indexerModel.getIndex(event.getIndexName());
                            if (shouldRunIndexUpdater(index)) {
                                if (indexUpdaters.containsKey(index.getName())) {
                                    updateIndexUpdater(index);
                                } else {
                                    addIndexUpdater(index);
                                }
                            } else {
                                removeIndexUpdater(index.getName());
                            }
                        } catch (IndexNotFoundException e) {
                            removeIndexUpdater(event.getIndexName());
                        } catch (Throwable t) {
                            log.error("Error in IndexerWorker's IndexerModelListener.", t);
                        }
                    } else if (event.getType() == INDEX_REMOVED) {
                        removeIndexUpdater(event.getIndexName());
                    }
                } catch (InterruptedException e) {
                    log.info("IndexerWorker.EventWorker interrupted.");
                    return;
                } catch (Throwable t) {
                    log.error("Error processing indexer model event in IndexerWorker.", t);
                }
            }
        }
    }
}
