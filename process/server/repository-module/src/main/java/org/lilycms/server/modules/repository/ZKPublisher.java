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

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;
import org.lilycms.util.zookeeper.ZooKeeperOperation;

/**
 * Publishes this Lily repository node to Zookeeper.
 *
 */
public class ZKPublisher {
    private ZooKeeperItf zk;
    private String hostAddress;
    private int port;
    private String lilyPath = "/lily";
    private String nodesPath = lilyPath + "/repositoryNodes";
    private String blobDfsUriPath = lilyPath + "/blobStoresConfig/dfsUri";
    private String blobHBaseZkQuorumPath = lilyPath + "/blobStoresConfig/hbaseZkQuorum";
    private String blobHBaseZkPortPath = lilyPath + "/blobStoresConfig/hbaseZkPort";
    private final String dfsUri;
    private final Configuration hbaseConf;

    public ZKPublisher(ZooKeeperItf zk, String hostAddress, int port, String dfsUri, Configuration hbaseConf) {
        this.zk = zk;
        this.hostAddress = hostAddress;
        this.port = port;
        this.dfsUri = dfsUri;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws IOException, InterruptedException, KeeperException {
        publishBlobSetup();

        // Publish our address
        ZkUtil.createPath(zk, nodesPath);
        final String repoAddressAndPort = hostAddress + ":" + port;
        ZkUtil.retryOperationForever(new ZooKeeperOperation<Object>() {
            public Object execute() throws KeeperException, InterruptedException {
                zk.create(nodesPath + "/" + repoAddressAndPort, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return null;
            }
        });
    }

    private void publishBlobSetup() throws InterruptedException, KeeperException {
        // The below serves as a stop-gap solution for the blob configuration: we store the information in ZK
        // that clients need to know how to access the blob store locations, but the actual setup of the
        // BlobStoreAccessFactory is currently hardcoded
        ZkUtil.createPath(zk, blobDfsUriPath, dfsUri.getBytes(), CreateMode.PERSISTENT);
        ZkUtil.createPath(zk, blobHBaseZkQuorumPath, hbaseConf.get("hbase.zookeeper.quorum").getBytes(), CreateMode.PERSISTENT);
        ZkUtil.createPath(zk, blobHBaseZkPortPath, hbaseConf.get("hbase.zookeeper.property.clientPort").getBytes(), CreateMode.PERSISTENT);
    }
}
