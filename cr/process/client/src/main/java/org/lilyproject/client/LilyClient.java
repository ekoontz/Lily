/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.BlobStoreAccessFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.avro.AvroConverter;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseBlobStoreAccess;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.repository.impl.InlineBlobStoreAccess;
import org.lilyproject.repository.impl.RemoteRepository;
import org.lilyproject.repository.impl.RemoteTypeManager;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.DfsUri;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * Provides remote repository implementations.
 *
 * <p>Connects to zookeeper to find out available repository nodes.
 *
 * <p>Each call to {@link #getRepository()} will return a server at random. If you are in a situation where the
 * number of clients is limited and clients are long-running (e.g. some front-end servers), you should frequently
 * request an new Repository object in order to avoid talking to the same server all the time.
 */
public class LilyClient implements Closeable {
    private ZooKeeperItf zk;
    private boolean managedZk;
    private List<ServerNode> servers = Collections.synchronizedList(new ArrayList<ServerNode>());
    private Set<String> serverAddresses = new HashSet<String>();
    private RetryConf retryConf = new RetryConf();
    private static final String nodesPath = "/lily/repositoryNodes";
    private static final String blobDfsUriPath = "/lily/blobStoresConfig/dfsUri";
    private static final String blobHBaseZkQuorumPath = "/lily/blobStoresConfig/hbaseZkQuorum";
    private static final String blobHBaseZkPortPath = "/lily/blobStoresConfig/hbaseZkPort";

    private Log log = LogFactory.getLog(getClass());

    private ZkWatcher watcher = new ZkWatcher();

    private Repository balancingAndRetryingRepository = BalancingAndRetryingRepository.getInstance(this);

    public LilyClient(ZooKeeperItf zk) throws IOException, InterruptedException, KeeperException,
            ZkConnectException, NoServersException {
        this.zk = zk;
        init();
    }

    /**
     *
     * @throws NoServersException if the znode under which the repositories are published does not exist
     */
    public LilyClient(String zookeeperConnectString, int sessionTimeout) throws IOException, InterruptedException,
            KeeperException, ZkConnectException, NoServersException {
        managedZk = true;
        zk = ZkUtil.connect(zookeeperConnectString, sessionTimeout);
        init();
    }

    private void init() throws InterruptedException, KeeperException, NoServersException {
        zk.addDefaultWatcher(watcher);
        refreshServers();
        Stat stat = zk.exists(nodesPath, false);
        if (stat == null) {
            throw new NoServersException("Repositories znode does not exist in ZooKeeper: " + nodesPath);
        }
    }

    public void close() throws IOException {
        zk.removeDefaultWatcher(watcher);

        for (ServerNode node : servers) {
            Closer.close(node.repository);
        }

        if (managedZk && zk != null) {
            zk.close();
        }
    }

    /**
     * Returns a Repository that uses one of the available Lily servers (randomly selected).
     * This repository instance will not automatically retry operations and to balance requests
     * over multiple Lily servers, you need to recall this method regularly to retrieve other
     * repository instances. Most of the time, you will rather use {@link #getRepository()}.
     */
    public synchronized Repository getPlainRepository() throws IOException, NoServersException, InterruptedException, KeeperException {
        if (servers.size() == 0) {
            throw new NoServersException("No servers available");
        }
        int pos = (int)Math.floor(Math.random() * servers.size());
        ServerNode server = servers.get(pos);
        if (server.repository == null) {
            constructRepository(server);
        }
        return server.repository;
    }

    /**
     * Returns a repository instance which will automatically balance requests over the available
     * Lily servers, and will retry operations according to what is specified in {@link RetryConf}.
     *
     * <p>To see some information when the client goes into retry mode, enable INFO logging for
     * the category org.lilyproject.client.
     */
    public Repository getRepository() {
        return balancingAndRetryingRepository;
    }

    public RetryConf getRetryConf() {
        return retryConf;
    }

    public void setRetryConf(RetryConf retryConf) {
        this.retryConf = retryConf;
    }

    private void constructRepository(ServerNode server) throws IOException, InterruptedException, KeeperException {
        AvroConverter remoteConverter = new AvroConverter();
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
        RemoteTypeManager typeManager = new RemoteTypeManager(parseAddressAndPort(server.lilyAddressAndPort),
                remoteConverter, idGenerator, zk);
        
        BlobStoreAccessFactory blobStoreAccessFactory = getBlobStoreAccess(zk);
        
        Repository repository = new RemoteRepository(parseAddressAndPort(server.lilyAddressAndPort),
                remoteConverter, typeManager, idGenerator, blobStoreAccessFactory);
        
        remoteConverter.setRepository(repository);
        typeManager.start();
        server.repository = repository;
    }

    public static BlobStoreAccessFactory getBlobStoreAccess(ZooKeeperItf zk) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", getBlobHBaseZkQuorum(zk));
        configuration.set("hbase.zookeeper.property.clientPort", getBlobHBaseZkPort(zk));

        URI dfsUri = getDfsUri(zk);
        FileSystem fs = FileSystem.get(DfsUri.getBaseDfsUri(dfsUri), configuration);
        Path blobRootPath = new Path(DfsUri.getDfsPath(dfsUri));

        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(fs, blobRootPath);
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(configuration);
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess();
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        blobStoreAccessFactory.addBlobStoreAccess(5000, inlineBlobStoreAccess);
        blobStoreAccessFactory.addBlobStoreAccess(200000, hbaseBlobStoreAccess);
        return blobStoreAccessFactory;
    }
    
    private static URI getDfsUri(ZooKeeperItf zk)  {
        try {
            return new URI(new String(zk.getData(blobDfsUriPath, false, new Stat())));
        } catch (Exception e) {
            throw new RuntimeException("Blob stores config lookup: failed to get DFS URI from ZooKeeper", e);
        }
    }

    private static String getBlobHBaseZkQuorum(ZooKeeperItf zk) {
        try {
            return new String(zk.getData(blobHBaseZkQuorumPath, false, new Stat()));
        } catch (Exception e) {
            throw new RuntimeException("Blob stores config lookup: failed to get HBase ZooKeeper quorum from ZooKeeper", e);
        }
    }

    private static String getBlobHBaseZkPort(ZooKeeperItf zk) {
        try {
            return new String(zk.getData(blobHBaseZkPortPath, false, new Stat()));
        } catch (Exception e) {
            throw new RuntimeException("Blob stores config lookup: failed to get HBase ZooKeeper port from ZooKeeper", e);
        }
    }

    private InetSocketAddress parseAddressAndPort(String addressAndPort) {
        int colonPos = addressAndPort.indexOf(":");
        if (colonPos == -1) {
            // since these are produced by the server nodes, this should never occur
            throw new RuntimeException("Unexpected situation: invalid addressAndPort: " + addressAndPort);
        }

        String address = addressAndPort.substring(0, colonPos);
        int port = Integer.parseInt(addressAndPort.substring(colonPos + 1));

        return new InetSocketAddress(address, port);
    }

    private class ServerNode {
        private String lilyAddressAndPort;
        private Repository repository;

        public ServerNode(String lilyAddressAndPort) {
            this.lilyAddressAndPort = lilyAddressAndPort;
        }
    }

    private synchronized void refreshServers() throws InterruptedException, KeeperException {
        Set<String> currentServers = new HashSet<String>();
        currentServers.addAll(zk.getChildren(nodesPath, true));

        Set<String> removedServers = new HashSet<String>();
        removedServers.addAll(serverAddresses);
        removedServers.removeAll(currentServers);

        Set<String> newServers = new HashSet<String>();
        newServers.addAll(currentServers);
        newServers.removeAll(serverAddresses);

        if (log.isDebugEnabled()) {
            log.debug("# current servers in ZK: " + currentServers.size() + ", # added servers: " +
                    newServers.size() + ", # removed servers: " + removedServers.size());
        }

        // Remove removed servers
        Iterator<ServerNode> serverIt = servers.iterator();
        while (serverIt.hasNext()) {
            ServerNode server = serverIt.next();
            if (removedServers.contains(server.lilyAddressAndPort)) {
                serverIt.remove();
            }
        }
        serverAddresses.removeAll(removedServers);

        // Add new servers
        for (String server : newServers) {
            servers.add(new ServerNode(server));
            serverAddresses.add(server);
        }

        if (log.isInfoEnabled()) {
            log.info("Current Lily servers = " + serverAddresses.toString());
        }
    }

    private synchronized void clearServers() {
        if (log.isInfoEnabled()) {
            log.info("Not connected to ZooKeeper, will clear list of servers.");
        }
        servers.clear();
        serverAddresses.clear();
    }

    private class ZkWatcher implements Watcher {
        public void process(WatchedEvent event) {
            try {
                if (event.getState() != Event.KeeperState.SyncConnected) {
                    clearServers();
                } else {
                    // We refresh the servers not only when /lily/repositoryNodes has changed, but also
                    // when we get a SyncConnected event, since our watcher might not be installed anymore
                    // (we do not have to check if it was still installed: ZK ignores double registration
                    // of the same watcher object)
                    refreshServers();
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (KeeperException.ConnectionLossException e) {
                clearServers();
            } catch (Throwable t) {
                log.error("Error in ZooKeeper watcher.", t);
            }
        }
    }
}
