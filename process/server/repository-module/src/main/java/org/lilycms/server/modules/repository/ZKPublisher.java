package org.lilycms.server.modules.repository;

import org.apache.zookeeper.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * Publishes this Lily repository node to Zookeeper.
 *
 * <p>TODO this should do more than publishing, e.g. in case connection to ZK is lost
 * we should probably stop handling clien requests?
 */
public class ZKPublisher {
    private String zkConnectString;
    private String hostAddress;
    private int port;
    private ZooKeeper zk;
    private String lilyPath = "/lily";
    private String nodesPath = lilyPath + "/repositoryNodes";

    public ZKPublisher(String zkConnectString, String hostAddress, int port) {
        this.zkConnectString = zkConnectString;
        this.hostAddress = hostAddress;
        this.port = port;
    }

    @PostConstruct
    public void start() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(zkConnectString, 5000, new ZkWatcher());
        try {
            zk.create(lilyPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }

        try {
            zk.create(nodesPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }

        String repoAddressAndPort = hostAddress + ":" + port;

        zk.create(nodesPath + "/" + repoAddressAndPort, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        zk.close();
    }

    private static class ZkWatcher implements Watcher {
        public void process(WatchedEvent watchedEvent) {
            System.out.println("Got zookeeper event: " + watchedEvent);
        }
    }
}
