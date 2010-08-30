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
package org.lilycms.server.modules.repository;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.lilycms.util.zookeeper.ZkPathCreationException;
import org.lilycms.util.zookeeper.ZkUtil;

/**
 * Publishes this Lily repository node to Zookeeper.
 *
 * <p>TODO this should do more than publishing, e.g. in case connection to ZK is lost
 * we should probably stop handling client requests?
 */
public class ZKPublisher {
    private String zkConnectString;
    private String hostAddress;
    private int port;
    private ZooKeeper zk;
    private String lilyPath = "/lily";
    private String nodesPath = lilyPath + "/repositoryNodes";
    private String blobDfsUriPath = lilyPath + "/blobStoresConfig/dfsUri";
    private String blobHBaseZkQuorumPath = lilyPath + "/blobStoresConfig/hbaseZkQuorum";
    private String blobHBaseZkPortPath = lilyPath + "/blobStoresConfig/hbaseZkPort";
    private final String dfsUri;
    private final Configuration hbaseConf;

    public ZKPublisher(String zkConnectString, String hostAddress, int port, String dfsUri, Configuration hbaseConf) {
        this.zkConnectString = zkConnectString;
        this.hostAddress = hostAddress;
        this.port = port;
        this.dfsUri = dfsUri;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws IOException, InterruptedException, KeeperException, ZkPathCreationException {
        zk = new ZooKeeper(zkConnectString, 5000, new ZkWatcher());

        ZkUtil.createPath(zk, nodesPath);
        String repoAddressAndPort = hostAddress + ":" + port;
        zk.create(nodesPath + "/" + repoAddressAndPort, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // The below serves as a stop-gap solution for the blob configuration: we store the information in ZK
        // that clients need to know how to access the blob store locations, but the actual setup of the
        // BlobStoreAccessFactory is currently hardcoded
        ZkUtil.createPath(zk, blobDfsUriPath);
        zk.setData(blobDfsUriPath, dfsUri.getBytes(), -1);
        ZkUtil.createPath(zk, blobHBaseZkQuorumPath);
        zk.setData(blobHBaseZkQuorumPath, hbaseConf.get("hbase.zookeeper.quorum").getBytes(), -1);
        ZkUtil.createPath(zk, blobHBaseZkPortPath);
        zk.setData(blobHBaseZkPortPath, hbaseConf.get("hbase.zookeeper.property.clientPort").getBytes(), -1);
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
