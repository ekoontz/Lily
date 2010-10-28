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
package org.lilyproject.server.modules.rowlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.rowlog.api.*;
import org.lilyproject.rowlog.impl.*;
import org.lilyproject.util.hbase.HBaseTableUtil;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

public class RowLogSetup {
    private final RowLogConfigurationManager confMgr;
    private final Configuration hbaseConf;
    private final ZooKeeperItf zk;
    private RowLogImpl messageQueue;
    private RowLogImpl writeAheadLog;
    private RowLogProcessorElection messageQueueProcessorLeader;

    public RowLogSetup(RowLogConfigurationManager confMgr, ZooKeeperItf zk, Configuration hbaseConf) {
        this.confMgr = confMgr;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, LeaderElectionSetupException {
        // If the subscription already exists, this method will silently return
        if (!confMgr.subscriptionExists("WAL", "LinkIndexUpdater")) {
            confMgr.addSubscription("WAL", "LinkIndexUpdater", RowLogSubscription.Type.VM, 3, 10);
        }
        if (!confMgr.subscriptionExists("WAL", "MQFeeder")) {
            confMgr.addSubscription("WAL", "MQFeeder", RowLogSubscription.Type.VM, 3, 20);
        }

        messageQueue = new RowLogImpl("mq", HBaseTableUtil.getRecordTable(hbaseConf), HBaseTableUtil.MQ_PAYLOAD_COLUMN_FAMILY,
                HBaseTableUtil.MQ_COLUMN_FAMILY, 10000L, true, confMgr);
        messageQueue.registerShard(new RowLogShardImpl("shard1", hbaseConf, messageQueue, 100));

        writeAheadLog = new RowLogImpl("wal", HBaseTableUtil.getRecordTable(hbaseConf), HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY,
                HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, true, confMgr);
        RowLogShard walShard = new RowLogShardImpl("shard1", hbaseConf, writeAheadLog, 100);
        writeAheadLog.registerShard(walShard);

        RowLogMessageListenerMapping.INSTANCE.put("MQFeeder", new MessageQueueFeeder(messageQueue));

        // Start the processor
        messageQueueProcessorLeader = new RowLogProcessorElection(zk, new RowLogProcessorImpl(messageQueue, confMgr));
        messageQueueProcessorLeader.start();
    }

    @PreDestroy
    public void stop() {
        messageQueueProcessorLeader.stop();
        messageQueue.stop();
        writeAheadLog.stop();
        RowLogMessageListenerMapping listenerClassMapping = RowLogMessageListenerMapping.INSTANCE;
        listenerClassMapping.remove("MQFeeder");        
    }

    public RowLog getMessageQueue() {
        return messageQueue;
    }

    public RowLog getWriteAheadLog() {
        return writeAheadLog;
    }
}
