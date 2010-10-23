package org.lilyproject.indexer.model.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import static org.apache.zookeeper.Watcher.Event.EventType.*;
import org.lilyproject.indexer.model.api.*;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilyproject.indexer.model.sharding.ShardSelector;
import org.lilyproject.indexer.model.sharding.ShardingConfigException;
import org.lilyproject.util.Logs;
import org.lilyproject.util.ObjectUtils;
import org.lilyproject.util.zookeeper.*;

import javax.annotation.PreDestroy;

import static org.lilyproject.indexer.model.api.IndexerModelEventType.*;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// About how the indexer conf is stored in ZooKeeper
// -------------------------------------------------
// I had to make the decision of whether to store all properties of an index in the
// data of one node, or rather to add these properties as subnodes.
//
// The advantages of putting index properties in subnodes are:
//  - they allow to watch inidividual properties, so you know which one changed
//  - each property can be updated individually
//  - there is no impact of big properties, like the indexerconf XML, on other
//    ones
//
// The advantages of putting all index properties in the data of one znode are:
//  - the atomic create or update of an index is easy/possible. ZK does not have
//    transactions. Intermediate state while performing updates is not visible.
//  - watching is simpler: just watch one node, rather than having to register
//    watches on every child node and on the parent for children changes.
//  - in practice it is usually still easy to know what individual properties
//    changed, by comparing with previous state you hold yourself.
//
// So the clear winner was to put all properties in the data of one znode.
// It is much easier: less work, reduced complexity, less chance for errors.
// Also, the state of indexes does not change frequently, so that the data
// of this znode is somewhat bigger is not really important.


public class IndexerModelImpl implements WriteableIndexerModel {
    private ZooKeeperItf zk;

    /**
     * Cache of the indexes as they are stored in ZK. Updated based on ZK watcher events. People who update
     * this cache should synchronize on {@link #indexes_lock}.
     */
    private Map<String, IndexDefinition> indexes = new ConcurrentHashMap<String, IndexDefinition>(16, 0.75f, 1);

    private final Object indexes_lock = new Object();

    private Set<IndexerModelListener> listeners = Collections.newSetFromMap(new IdentityHashMap<IndexerModelListener, Boolean>());

    private Watcher watcher = new IndexModelChangeWatcher();

    private Watcher connectStateWatcher = new ConnectStateWatcher();

    private IndexCacheRefresher indexCacheRefresher = new IndexCacheRefresher();

    private boolean stopped = false;

    private Log log = LogFactory.getLog(getClass());

    private static final String INDEX_COLLECTION_PATH = "/lily/indexer/index";

    private static final String INDEX_TRASH_PATH = "/lily/indexer/index-trash";

    private static final String INDEX_COLLECTION_PATH_SLASH = INDEX_COLLECTION_PATH + "/";

    public IndexerModelImpl(ZooKeeperItf zk) throws InterruptedException, KeeperException {
        this.zk = zk;
        ZkUtil.createPath(zk, INDEX_COLLECTION_PATH);
        ZkUtil.createPath(zk, INDEX_TRASH_PATH);

        zk.addDefaultWatcher(connectStateWatcher);

        indexCacheRefresher.start();
        indexCacheRefresher.waitUntilStarted();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        stopped = true;
        zk.removeDefaultWatcher(connectStateWatcher);
        indexCacheRefresher.shutdown();
    }

    public IndexDefinition newIndex(String name) {
        return new IndexDefinitionImpl(name);
    }

    public void addIndex(IndexDefinition index) throws IndexExistsException, IndexModelException, IndexValidityException {
        assertValid(index);        

        final String indexPath = INDEX_COLLECTION_PATH + "/" + index.getName();
        final byte[] data = IndexDefinitionConverter.INSTANCE.toJsonBytes(index);

        try {
            zk.retryOperation(new ZooKeeperOperation<String>() {
                public String execute() throws KeeperException, InterruptedException {
                    return zk.create(indexPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            });
        } catch (KeeperException.NodeExistsException e) {
            throw new IndexExistsException(index.getName());
        } catch (Exception e) {
            throw new IndexModelException("Error creating index.", e);
        }
    }

    private void assertValid(IndexDefinition index) throws IndexValidityException {
        if (index.getName() == null || index.getName().length() == 0)
            throw new IndexValidityException("Name should not be null or zero-length");

        if (index.getConfiguration() == null)
            throw new IndexValidityException("Configuration should not be null.");

        if (index.getGeneralState() == null)
            throw new IndexValidityException("General state should not be null.");

        if (index.getBatchBuildState() == null)
            throw new IndexValidityException("Build state should not be null.");

        if (index.getUpdateState() == null)
            throw new IndexValidityException("Update state should not be null.");

        if (index.getActiveBatchBuildInfo() != null) {
            ActiveBatchBuildInfo info = index.getActiveBatchBuildInfo();
            if (info.getJobId() == null)
                throw new IndexValidityException("Job id of active batch build cannot be null.");
        }

        if (index.getLastBatchBuildInfo() != null) {
            BatchBuildInfo info = index.getLastBatchBuildInfo();
            if (info.getJobId() == null)
                throw new IndexValidityException("Job id of last batch build cannot be null.");
            if (info.getJobState() == null)
                throw new IndexValidityException("Job state of last batch build cannot be null.");
        }

        if (index.getSolrShards() == null || index.getSolrShards().isEmpty())
            throw new IndexValidityException("SOLR shards should not be null or empty.");

        for (String shard : index.getSolrShards().values()) {
            try {
                URI uri = new URI(shard);
                if (!uri.isAbsolute()) {
                    throw new IndexValidityException("SOLR shard URI is not absolute: " + shard);
                }
            } catch (URISyntaxException e) {
                throw new IndexValidityException("Invalid SOLR shard URI: " + shard);
            }
        }

        if (index.getShardingConfiguration() != null) {
            // parse it + check used shards -> requires dependency on the engine or moving the relevant classes
            // to the model
            ShardSelector selector;
            try {
                selector = JsonShardSelectorBuilder.build(index.getShardingConfiguration());
            } catch (ShardingConfigException e) {
                throw new IndexValidityException("Error with sharding configuration.", e);
            }

            Set<String> shardNames = index.getSolrShards().keySet();

            for (String shard : selector.getShards()) {
                if (!shardNames.contains(shard)) {
                    throw new IndexValidityException("The sharding configuration refers to a shard that is not" +
                    " in the set of available shards. Shard: " + shard);
                }
            }
        }

        try {
            IndexerConfBuilder.validate(new ByteArrayInputStream(index.getConfiguration()));
        } catch (IndexerConfException e) {
            throw new IndexValidityException("The indexer configuration is not XML well-formed or valid.", e);
        }
    }

    public void updateIndexInternal(final IndexDefinition index) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, IndexValidityException {

        assertValid(index);

        final byte[] newData = IndexDefinitionConverter.INSTANCE.toJsonBytes(index);

        try {
            zk.retryOperation(new ZooKeeperOperation<Stat>() {
                public Stat execute() throws KeeperException, InterruptedException {
                    return zk.setData(INDEX_COLLECTION_PATH_SLASH + index.getName(), newData, index.getZkDataVersion());
                }
            });
        } catch (KeeperException.NoNodeException e) {
            throw new IndexNotFoundException(index.getName());
        } catch (KeeperException.BadVersionException e) {
            throw new IndexConcurrentModificationException(index.getName());
        }
    }

    public void updateIndex(final IndexDefinition index, String lock) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, ZkLockException, IndexUpdateException,
            IndexValidityException {

        if (!ZkLock.ownsLock(zk, lock)) {
            throw new IndexUpdateException("You are not owner of the indexes lock, your lock path is: " + lock);
        }

        assertValid(index);

        IndexDefinition currentIndex = getMutableIndex(index.getName());

        if (currentIndex.getGeneralState() == IndexGeneralState.DELETE_REQUESTED ||
                currentIndex.getGeneralState() == IndexGeneralState.DELETING) {
            throw new IndexUpdateException("An index in state " + index.getGeneralState() + " cannot be modified.");
        }

        if (index.getBatchBuildState() == IndexBatchBuildState.BUILD_REQUESTED &&
                currentIndex.getBatchBuildState() != IndexBatchBuildState.INACTIVE) {
            throw new IndexUpdateException("Cannot move index build state from " + currentIndex.getBatchBuildState() +
                    " to " + index.getBatchBuildState());
        }

        if (currentIndex.getGeneralState() == IndexGeneralState.DELETE_REQUESTED) {
            throw new IndexUpdateException("An index in the state " + IndexGeneralState.DELETE_REQUESTED +
                    " cannot be updated.");
        }

        if (!ObjectUtils.safeEquals(currentIndex.getActiveBatchBuildInfo(), index.getActiveBatchBuildInfo())) {
            throw new IndexUpdateException("The active batch build info cannot be modified by users.");
        }

        if (!ObjectUtils.safeEquals(currentIndex.getLastBatchBuildInfo(), index.getLastBatchBuildInfo())) {
            throw new IndexUpdateException("The last batch build info cannot be modified by users.");
        }

        updateIndexInternal(index);

    }

    public void deleteIndex(final String indexName) throws IndexModelException {
        final String indexPath = INDEX_COLLECTION_PATH_SLASH + indexName;
        final String indexLockPath = indexPath + "/lock";

        try {
            // Make a copy of the index data in the index trash
            zk.retryOperation(new ZooKeeperOperation<Object>() {
                public Object execute() throws KeeperException, InterruptedException {
                    byte[] data = zk.getData(indexPath, false, null);

                    String trashPath = INDEX_TRASH_PATH + "/" + indexName;

                    // An index with the same name might have existed before and hence already exist
                    // in the index trash, handle this by appending a sequence number until a unique
                    // name is found.
                    String baseTrashpath = trashPath;
                    int count = 0;
                    while (zk.exists(trashPath, false) != null) {
                        count++;
                        trashPath = baseTrashpath + "." + count;
                    }

                    zk.create(trashPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    return null;
                }
            });

            // The loop below is normally not necessary, since we disallow taking new locks on indexes
            // which are being deleted.
            int tryCount = 0;
            while (true) {
                boolean success = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                    public Boolean execute() throws KeeperException, InterruptedException {
                        try {
                            // Delete the index lock if it exists
                            if (zk.exists(indexLockPath, false) != null) {
                                List<String> children = Collections.emptyList();
                                try {
                                    children = zk.getChildren(indexLockPath, false);
                                } catch (KeeperException.NoNodeException e) {
                                    // ok
                                }

                                for (String child : children) {
                                    try {
                                        zk.delete(indexLockPath + "/" + child, -1);
                                    } catch (KeeperException.NoNodeException e) {
                                        // ignore, node was already removed
                                    }
                                }

                                try {
                                    zk.delete(indexLockPath, -1);
                                } catch (KeeperException.NoNodeException e) {
                                    // ignore
                                }
                            }


                            zk.delete(indexPath, -1);

                            return true;
                        } catch (KeeperException.NotEmptyException e) {
                            // Someone again took a lock on the index, retry
                        }
                        return false;
                    }
                });

                if (success)
                    break;
                
                tryCount++;
                if (tryCount > 10) {
                    throw new IndexModelException("Failed to delete index because it still has child data. Index: " + indexName);
                }
            }
        } catch (Throwable t) {
            if (t instanceof InterruptedException)
                Thread.currentThread().interrupt();
            throw new IndexModelException("Failed to delete index " + indexName, t);
        }
    }

    public String lockIndexInternal(String indexName, boolean checkDeleted) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException, IndexModelException {

        IndexDefinition index = getIndex(indexName);

        if (checkDeleted) {
            if (index.getGeneralState() == IndexGeneralState.DELETE_REQUESTED ||
                    index.getGeneralState() == IndexGeneralState.DELETING) {
                throw new IndexModelException("An index in state " + index.getGeneralState() + " cannot be locked.");
            }
        }

        final String lockPath = INDEX_COLLECTION_PATH_SLASH + indexName + "/lock";

        //
        // Create the lock path if necessary
        //
        Stat stat = zk.retryOperation(new ZooKeeperOperation<Stat>() {
            public Stat execute() throws KeeperException, InterruptedException {
                return zk.exists(lockPath, null);
            }
        });

        if (stat == null) {
            // We do not make use of ZkUtil.createPath (= recursive path creation) on purpose,
            // because if the parent path does not exist, this means the index does not exist,
            // and we do not want to create an index path (with null data) like that.
            try {
                zk.retryOperation(new ZooKeeperOperation<String>() {
                    public String execute() throws KeeperException, InterruptedException {
                        return zk.create(lockPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                });
            } catch (KeeperException.NodeExistsException e) {
                // ok, someone created it since we checked
            } catch (KeeperException.NoNodeException e) {
                throw new IndexNotFoundException(indexName);
            }
        }

        //
        // Take the actual lock
        //
        return ZkLock.lock(zk, INDEX_COLLECTION_PATH_SLASH + indexName + "/lock");

    }


    public String lockIndex(String indexName) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException, IndexModelException {
        return lockIndexInternal(indexName, true);
    }

    public void unlockIndex(String lock) throws ZkLockException {
        ZkLock.unlock(zk, lock);
    }

    public void unlockIndex(String lock, boolean ignoreMissing) throws ZkLockException {
        ZkLock.unlock(zk, lock, ignoreMissing);
    }

    public IndexDefinition getIndex(String name) throws IndexNotFoundException {
        IndexDefinition index = indexes.get(name);
        if (index == null) {
            throw new IndexNotFoundException(name);
        }
        return index;
    }

    public boolean hasIndex(String name) {
        return indexes.containsKey(name);
    }

    public IndexDefinition getMutableIndex(String name) throws InterruptedException, KeeperException, IndexNotFoundException {
        return loadIndex(name, false);
    }

    public Collection<IndexDefinition> getIndexes() {
        return new ArrayList<IndexDefinition>(indexes.values());
    }

    public Collection<IndexDefinition> getIndexes(IndexerModelListener listener) {
        synchronized (indexes_lock) {
            registerListener(listener);
            return new ArrayList<IndexDefinition>(indexes.values());
        }
    }

    private IndexDefinitionImpl loadIndex(String indexName, boolean forCache)
            throws InterruptedException, KeeperException, IndexNotFoundException {
        final String childPath = INDEX_COLLECTION_PATH + "/" + indexName;
        final Stat stat = new Stat();

        byte[] data;
        try {
            if (forCache) {
                // do not retry, install watcher
                data = zk.getData(childPath, watcher, stat);
            } else {
                // do retry, do not install watcher
                data = zk.retryOperation(new ZooKeeperOperation<byte[]>() {
                    public byte[] execute() throws KeeperException, InterruptedException {
                        return zk.getData(childPath, false, stat);
                    }
                });
            }
        } catch (KeeperException.NoNodeException e) {
            throw new IndexNotFoundException(indexName);
        }

        IndexDefinitionImpl index = new IndexDefinitionImpl(indexName);
        index.setZkDataVersion(stat.getVersion());
        IndexDefinitionConverter.INSTANCE.fromJsonBytes(data, index);

        return index;
    }

    private void notifyListeners(List<IndexerModelEvent> events) {
        for (IndexerModelEvent event : events) {
            for (IndexerModelListener listener : listeners) {
                listener.process(event);
            }
        }
    }

    public void registerListener(IndexerModelListener listener) {
        this.listeners.add(listener);
    }

    public void unregisterListener(IndexerModelListener listener) {
        this.listeners.remove(listener);
    }

    private class IndexModelChangeWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (stopped) {
                return;
            }

            try {
                if (NodeChildrenChanged.equals(event.getType()) && event.getPath().equals(INDEX_COLLECTION_PATH)) {
                    indexCacheRefresher.triggerRefreshAllIndexes();
                } else if (NodeDataChanged.equals(event.getType()) && event.getPath().startsWith(INDEX_COLLECTION_PATH_SLASH)) {
                    String indexName = event.getPath().substring(INDEX_COLLECTION_PATH_SLASH.length());
                    indexCacheRefresher.triggerIndexToRefresh(indexName);
                }
            } catch (Throwable t) {
                log.error("Indexer Model: error handling event from ZooKeeper. Event: " + event, t);
            }
        }
    }

    public class ConnectStateWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (stopped) {
                return;
            }

            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                // Each time the connection is established, we trigger refreshing, since the previous refresh
                // might have failed with a ConnectionLoss exception
                indexCacheRefresher.triggerRefreshAllIndexes();
            }
        }
    }

    /**
     * Responsible for updating our internal cache of IndexDefinition's. Should be triggered upon each related
     * change on ZK, as well as on ZK connection established, since this refresher simply fails on ZK connection
     * loss exceptions, rather than retrying.
     */
    private class IndexCacheRefresher implements Runnable {
        private volatile Set<String> indexesToRefresh = new HashSet<String>();
        private volatile boolean refreshAllIndexes;
        private final Object refreshLock = new Object();
        private Thread thread;
        private final Object startedLock = new Object();
        private volatile boolean started = false;

        public synchronized void shutdown() throws InterruptedException {
            if (thread == null || !thread.isAlive()) {
                return;
            }

            thread.interrupt();
            Logs.logThreadJoin(thread);
            thread.join();
            thread = null;
        }

        public synchronized void start() {
            // Upon startup, be sure to run a refresh of all indexes
            this.refreshAllIndexes = true;

            thread = new Thread(this, "Indexer model refresher");
            // Set as daemon thread: IndexerModel can be used in tools like the indexer admin CLI tools,
            // where we should not require explicit shutdown.
            thread.setDaemon(true);
            thread.start();
        }

        /**
         * Waits until the initial cache fill up happened.
         */
        public void waitUntilStarted() throws InterruptedException {
            synchronized (startedLock) {
                while (!started) {
                    startedLock.wait();
                }
            }
        }

        public void run() {
            while (!Thread.interrupted()) {
                try {
                    List<IndexerModelEvent> events = new ArrayList<IndexerModelEvent>();
                    try {
                        Set<String> indexesToRefresh = null;
                        boolean refreshAllIndexes = false;

                        synchronized (refreshLock) {
                            if (this.refreshAllIndexes || this.indexesToRefresh.isEmpty()) {
                                refreshAllIndexes = true;
                            } else {
                                indexesToRefresh = new HashSet<String>(this.indexesToRefresh);
                            }
                            this.refreshAllIndexes = false;
                            this.indexesToRefresh.clear();
                        }

                        if (refreshAllIndexes) {
                            synchronized (indexes_lock) {
                                refreshIndexes(events);
                            }
                        } else {
                            synchronized (indexes_lock) {
                                for (String indexName : indexesToRefresh) {
                                    refreshIndex(indexName, events);
                                }
                            }
                        }

                        if (!started) {
                            started = true;
                            synchronized (startedLock) {
                                startedLock.notifyAll();
                            }
                        }
                    } finally {
                        // We notify the listeners here because we want to be sure events for every
                        // change are delivered, even if halfway through the refreshing we would have
                        // failed due to some error like a ZooKeeper connection loss
                        if (!events.isEmpty() && !stopped && !Thread.currentThread().isInterrupted()) {
                            notifyListeners(events);
                        }
                    }

                    synchronized (refreshLock) {
                        if (!this.refreshAllIndexes && this.indexesToRefresh.isEmpty()) {
                            refreshLock.wait();
                        }
                    }
                } catch (KeeperException.ConnectionLossException e) {
                    // we will be retriggered when the connection is back
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    log.error("Indexer Model Refresher: some exception happened.", t);
                }
            }
        }

        public void triggerIndexToRefresh(String indexName) {
            synchronized (refreshLock) {
                indexesToRefresh.add(indexName);
                refreshLock.notifyAll();
            }
        }

        public void triggerRefreshAllIndexes() {
            synchronized (refreshLock) {
                refreshAllIndexes = true;
                refreshLock.notifyAll();
            }
        }

        private void refreshIndexes(List<IndexerModelEvent> events) throws InterruptedException, KeeperException {
            List<String> indexNames = zk.getChildren(INDEX_COLLECTION_PATH, watcher);

            Set<String> indexNameSet = new HashSet<String>();
            indexNameSet.addAll(indexNames);

            // Remove indexes which no longer exist in ZK
            Iterator<String> currentIndexNamesIt = indexes.keySet().iterator();
            while (currentIndexNamesIt.hasNext()) {
                String indexName = currentIndexNamesIt.next();
                if (!indexNameSet.contains(indexName)) {
                    currentIndexNamesIt.remove();
                    events.add(new IndexerModelEvent(INDEX_REMOVED, indexName));
                }
            }

            // Add/update the other indexes
            for (String indexName : indexNames) {
                refreshIndex(indexName, events);
            }
        }

        /**
         * Adds or updates the given index to the internal cache.
         */
        private void refreshIndex(final String indexName, List<IndexerModelEvent> events)
                throws InterruptedException, KeeperException {
            try {
                IndexDefinitionImpl index = loadIndex(indexName, true);
                index.makeImmutable();

                IndexDefinition oldIndex = indexes.get(indexName);

                if (oldIndex != null && oldIndex.getZkDataVersion() == index.getZkDataVersion()) {
                    // nothing changed
                } else {
                    final boolean isNew = oldIndex == null;
                    indexes.put(indexName, index);
                    events.add(new IndexerModelEvent(isNew ? IndexerModelEventType.INDEX_ADDED : IndexerModelEventType.INDEX_UPDATED, indexName));
                }
            } catch (IndexNotFoundException e) {
                Object oldIndex = indexes.remove(indexName);

                if (oldIndex != null) {
                    events.add(new IndexerModelEvent(IndexerModelEventType.INDEX_REMOVED, indexName));
                }
            }
        }
    }
}
