package org.lilycms.indexer.worker;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.lilycms.hbaseindex.*;
import org.lilycms.indexer.engine.*;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.IndexNotFoundException;
import org.lilycms.indexer.model.indexerconf.IndexerConf;
import org.lilycms.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilycms.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.ShardSelector;
import org.lilycms.indexer.model.api.*;
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.repository.api.Repository;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogConfigurationManager;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.impl.RemoteListenerHandler;
import org.lilycms.util.Logs;
import org.lilycms.util.ObjectUtils;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import static org.lilycms.indexer.model.api.IndexerModelEventType.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * IndexerWorker is responsible for the incremental indexing updating, thus for starting
 * index updaters on each Lily node for each index that is configured for updating (according
 * to its {@link IndexUpdateState}).
 *
 * <p>IndexerWorker does not shut down the index updaters when the ZooKeeper connection is
 * lost. This is in the assumption that if the ZK connection would be lost for a longer
 * period of time, the Lily node will shut down, and that it should not cause harm that
 * the index updaters continue to run according to a possibly outdated configuration, since
 * after all one can not expect these things to change momentarily.
 */
public class IndexerWorker {
    private IndexerModel indexerModel;

    private Repository repository;

    private LinkIndex linkIndex;

    private ZooKeeperItf zk;

    private RowLogConfigurationManager rowLogConfMgr;

    private RowLog rowLog;

    private final int listenersPerIndex;

    private IndexerModelListener listener = new MyListener();

    private Map<String, IndexUpdaterHandle> indexUpdaters = new HashMap<String, IndexUpdaterHandle>();

    private final Object indexUpdatersLock = new Object();

    private BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

    private Thread eventWorkerThread;

    private HttpClient httpClient;

    private MultiThreadedHttpConnectionManager connectionManager;
    
    private final Log log = LogFactory.getLog(getClass());

    public IndexerWorker(IndexerModel indexerModel, Repository repository, RowLog rowLog, ZooKeeperItf zk,
            Configuration hbaseConf, RowLogConfigurationManager rowLogConfMgr, int listenersPerIndex)
            throws IOException, org.lilycms.hbaseindex.IndexNotFoundException {
        this.indexerModel = indexerModel;
        this.repository = repository;
        this.rowLog = rowLog;
        this.linkIndex = new LinkIndex(new IndexManager(hbaseConf), repository);
        this.zk = zk;
        this.rowLogConfMgr = rowLogConfMgr;
        this.listenersPerIndex = listenersPerIndex;
    }

    @PostConstruct
    public void init() {
        connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.getParams().setDefaultMaxConnectionsPerHost(5);
        connectionManager.getParams().setMaxTotalConnections(50);
      	httpClient = new HttpClient(connectionManager);

        eventWorkerThread = new Thread(new EventWorker(), "IndexerWorkerEventWorker");
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
            Logs.logThreadJoin(eventWorkerThread);
            eventWorkerThread.join();
        } catch (InterruptedException e) {
            log.info("Interrupted while joining eventWorkerThread.");
        }

        for (IndexUpdaterHandle handle : indexUpdaters.values()) {
            try {
                handle.stop();
            } catch (InterruptedException e) {
                // Continue the stop procedure
            }
        }

        connectionManager.shutdown();
    }

    private void addIndexUpdater(IndexDefinition index) {
        IndexUpdaterHandle handle = null;
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
            IndexLocker indexLocker = new IndexLocker(zk);
            IndexerMetrics indexerMetrics = new IndexerMetrics(index.getName());
            Indexer indexer = new Indexer(indexerConf, repository, solrServers, indexLocker, indexerMetrics);

            IndexUpdaterMetrics updaterMetrics = new IndexUpdaterMetrics(index.getName());
            IndexUpdater indexUpdater = new IndexUpdater(indexer, repository, linkIndex, indexLocker, updaterMetrics);

            List<RemoteListenerHandler> listenerHandlers = new ArrayList<RemoteListenerHandler>();

            for (int i = 0; i < listenersPerIndex; i++) {
                RemoteListenerHandler handler = new RemoteListenerHandler(rowLog, index.getQueueSubscriptionId(),
                        indexUpdater, rowLogConfMgr);
                listenerHandlers.add(handler);
            }

            handle = new IndexUpdaterHandle(index, listenerHandlers, indexerMetrics, updaterMetrics);
            handle.start();

            indexUpdaters.put(index.getName(), handle);

            log.info("Started index updater for index " + index.getName());
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            log.error("Problem starting index updater for index " + index.getName(), t);

            if (handle != null) {
                // stop any listeners that might have been started
                try {
                    handle.stop();
                } catch (Throwable t2) {
                    log.error("Problem stopping listeners for failed-to-start index updater for index '" +
                            index.getName() + "'", t2);
                }
            }
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
            handle.stop();
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
                index.getQueueSubscriptionId() != null &&
                !index.getGeneralState().isDeleteState();
    }

    private class IndexUpdaterHandle {
        private IndexDefinition indexDef;
        private List<RemoteListenerHandler> listenerHandlers;
        private IndexerMetrics indexerMetrics;
        private IndexUpdaterMetrics updaterMetrics;

        public IndexUpdaterHandle(IndexDefinition indexDef, List<RemoteListenerHandler> listenerHandlers,
                IndexerMetrics indexerMetrics, IndexUpdaterMetrics updaterMetrics) {
            this.indexDef = indexDef;
            this.listenerHandlers = listenerHandlers;
            this.indexerMetrics = indexerMetrics;
            this.updaterMetrics = updaterMetrics;
        }

        public void start() throws RowLogException, InterruptedException, KeeperException {
            for (RemoteListenerHandler handler : listenerHandlers) {
                handler.start();
            }
        }

        public void stop() throws InterruptedException {
            for (RemoteListenerHandler handler : listenerHandlers) {
                handler.stop();
            }
            Closer.close(indexerMetrics);
            Closer.close(updaterMetrics);
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
