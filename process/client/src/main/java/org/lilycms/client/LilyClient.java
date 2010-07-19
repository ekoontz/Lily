package org.lilycms.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.avro.AvroConverter;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.RepositoryRemoteImpl;
import org.lilycms.repository.impl.TypeManagerRemoteImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Provides remote repository implementations.
 *
 * <p>Connects to zookeeper to find out available repository nodes.
 *
 * <p>Each call to {@link #getRepository()} will return a server at random. If you are in a situation where the
 * number of clients is limited and clients are long-running (e.g. some front-end servers), you should frequently
 * request an new Repository object in order to avoid talking to the same server all the time.
 */
public class LilyClient {
    private ZooKeeper zk;
    private List<ServerNode> servers = new ArrayList<ServerNode>();
    private Set<String> serverAddresses = new HashSet<String>();
    private String lilyPath = "/lily";
    private String nodesPath = lilyPath + "/repositoryNodes";

    private Log log = LogFactory.getLog(getClass());

    public LilyClient(String zookeeperConnectString) throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(zookeeperConnectString, 5000, new ZkWatcher());
        refreshServers();
    }

    public synchronized Repository getRepository() throws IOException, ServerUnavailableException {
        if (servers.size() == 0) {
            throw new ServerUnavailableException("No servers available");
        }
        int pos = (int)Math.floor(Math.random() * servers.size());
        ServerNode server = servers.get(pos);
        if (server.repository == null) {
            // TODO if this particular repository server would not be reachable, we could retry a number
            // of times with other servers instead.
            constructRepository(server);
        }
        return server.repository;
    }

    private void constructRepository(ServerNode server) throws IOException {
        AvroConverter remoteConverter = new AvroConverter();
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
        TypeManager typeManager = new TypeManagerRemoteImpl(parseAddressAndPort(server.lilyAddressAndPort),
                remoteConverter, idGenerator);
        Repository repository = new RepositoryRemoteImpl(parseAddressAndPort(server.lilyAddressAndPort),
                remoteConverter, (TypeManagerRemoteImpl)typeManager, idGenerator);
        remoteConverter.setRepository(repository);
        server.repository = repository;
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

    private synchronized void refreshServers() {
        Set<String> currentServers = new HashSet<String>();
        try {
            currentServers.addAll(zk.getChildren(nodesPath, true));
        } catch (Exception e) {
            log.error("Error querying list of Lily server nodes from Zookeeper.", e);
            return;
        }

        Set<String> removedServers = new HashSet<String>();
        removedServers.addAll(serverAddresses);
        removedServers.removeAll(currentServers);

        Set<String> newServers = new HashSet<String>();
        newServers.addAll(currentServers);
        newServers.removeAll(serverAddresses);

        if (log.isDebugEnabled()) {
            log.debug("ZK watcher: # current servers in ZK: " + currentServers.size() + ", # added servers: " +
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
    }

    private class ZkWatcher implements Watcher {
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getPath() != null && watchedEvent.getPath().equals(nodesPath)) {
                refreshServers();
            }
        }
    }
}
