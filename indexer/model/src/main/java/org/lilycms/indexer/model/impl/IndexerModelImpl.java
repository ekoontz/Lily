package org.lilycms.indexer.model.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import static org.apache.zookeeper.Watcher.Event.EventType.*;
import org.lilycms.indexer.model.api.*;
import org.lilycms.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.ShardSelector;
import org.lilycms.indexer.model.sharding.ShardingConfigException;
import org.lilycms.util.ObjectUtils;
import org.lilycms.util.zookeeper.*;
import static org.lilycms.indexer.model.api.IndexerModelEventType.*;

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

    /** Using a Map since there is no IdentitySet. */
    private Map<IndexerModelListener, Object> listeners = new IdentityHashMap<IndexerModelListener, Object>();

    private Watcher watcher = new MyWatcher();

    private Log log = LogFactory.getLog(getClass());

    private static final String INDEX_COLLECTION_PATH = "/lily/indexer/index";

    private static final String INDEX_COLLECTION_PATH_SLASH = INDEX_COLLECTION_PATH + "/";

    public IndexerModelImpl(ZooKeeperItf zk) throws ZkPathCreationException, InterruptedException, KeeperException {
        this.zk = zk;
        ZkUtil.createPath(zk, INDEX_COLLECTION_PATH);

        synchronized(indexes_lock) {
            refreshIndexes();
        }
    }

    public IndexDefinition newIndex(String name) {
        return new IndexDefinitionImpl(name);
    }

    public void addIndex(IndexDefinition index) throws IndexExistsException, IndexModelException, IndexValidityException {
        assertValid(index);        

        final String indexPath = INDEX_COLLECTION_PATH + "/" + index.getName();
        final byte[] data = IndexDefinitionConverter.INSTANCE.toJsonBytes(index);

        try {
            ZkUtil.retryOperationForever(new ZooKeeperOperation<String>() {
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
    }

    public void updateIndexInternal(final IndexDefinition index) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, IndexValidityException {

        assertValid(index);

        final byte[] newData = IndexDefinitionConverter.INSTANCE.toJsonBytes(index);

        try {
            ZkUtil.retryOperationForever(new ZooKeeperOperation<Stat>() {
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

    public String lockIndex(String indexName) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException {

        final String lockPath = INDEX_COLLECTION_PATH_SLASH + indexName + "/lock";

        //
        // Create the lock path if necessary
        //
        Stat stat = ZkUtil.retryOperationForever(new ZooKeeperOperation<Stat>() {
            public Stat execute() throws KeeperException, InterruptedException {
                return zk.exists(lockPath, null);
            }
        });

        if (stat == null) {
            // We do not make use of ZkUtil.createPath (= recursive path creation) on purpose,
            // because if the parent path does not exist, this means the index does not exist,
            // and we do not want to create an index path (with null data) like that.
            try {
                ZkUtil.retryOperationForever(new ZooKeeperOperation<String>() {
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

    public void unlockIndex(String lock) throws ZkLockException {
        ZkLock.unlock(zk, lock);
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
        return loadIndex(name);
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

    private List<IndexerModelEvent> refreshIndexes() throws InterruptedException, KeeperException {
        List<IndexerModelEvent> events = new ArrayList<IndexerModelEvent>();

        List<String> indexNames = ZkUtil.retryOperationForever(new ZooKeeperOperation<List<String>>() {
            public List<String> execute() throws KeeperException, InterruptedException {
                return zk.getChildren(INDEX_COLLECTION_PATH, watcher);
            }
        });

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

        // Add new indexes
        for (String indexName : indexNames) {
            if (!indexes.containsKey(indexName)) {
                events.add(refreshIndex(indexName));
            }
        }

        return events;
    }

    /**
     * Adds or updates the given index to the internal cache.
     */
    private IndexerModelEvent refreshIndex(final String indexName) throws InterruptedException, KeeperException {
        try {
            IndexDefinitionImpl index = loadIndex(indexName);
            index.makeImmutable();
            final boolean isNew = !indexes.containsKey(indexName);
            indexes.put(indexName, index);

            return new IndexerModelEvent(isNew ? IndexerModelEventType.INDEX_ADDED : IndexerModelEventType.INDEX_UPDATED, indexName);

        } catch (IndexNotFoundException e) {
            indexes.remove(indexName);

            return new IndexerModelEvent(IndexerModelEventType.INDEX_REMOVED, indexName);
        }
    }

    private IndexDefinitionImpl loadIndex(String indexName) throws InterruptedException, KeeperException, IndexNotFoundException {
        final String childPath = INDEX_COLLECTION_PATH + "/" + indexName;
        final Stat stat = new Stat();

        byte[] data;
        try {
            data = ZkUtil.retryOperationForever(new ZooKeeperOperation<byte[]>() {
                public byte[] execute() throws KeeperException, InterruptedException {
                    return zk.getData(childPath, watcher, stat);
                }
            });
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
            for (IndexerModelListener listener : listeners.keySet()) {
                listener.process(event);
            }
        }
    }

    public void registerListener(IndexerModelListener listener) {
        this.listeners.put(listener, null);
    }

    public void unregisterListener(IndexerModelListener listener) {
        this.listeners.remove(listener);
    }

    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
            try {
                if (NodeChildrenChanged.equals(event.getType()) && event.getPath().equals(INDEX_COLLECTION_PATH)) {

                    List<IndexerModelEvent> events;
                    synchronized (indexes_lock) {
                        events = refreshIndexes();
                    }

                    notifyListeners(events);

                } else if (NodeDataChanged.equals(event.getType()) && event.getPath().startsWith(INDEX_COLLECTION_PATH_SLASH)) {

                    IndexerModelEvent myEvent;
                    synchronized (indexes_lock) {
                        String indexName = event.getPath().substring(INDEX_COLLECTION_PATH_SLASH.length());
                        myEvent = refreshIndex(indexName);
                    }

                    notifyListeners(Collections.singletonList(myEvent));
                }
            } catch (Throwable t) {
                log.error("Indexer Model: error handling event from ZooKeeper. Event: " + event, t);
            }
        }
    }
}
