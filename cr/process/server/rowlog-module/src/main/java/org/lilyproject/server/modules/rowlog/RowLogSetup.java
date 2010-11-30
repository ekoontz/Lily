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
import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

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
    private RowLogProcessorElection writeAheadLogProcessorLeader;

    public RowLogSetup(RowLogConfigurationManager confMgr, ZooKeeperItf zk, Configuration hbaseConf) {
        this.confMgr = confMgr;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, LeaderElectionSetupException, RowLogException {
        // If the subscription already exists, this method will silently return
        if (!confMgr.subscriptionExists("wal", "LinkIndexUpdater")) {
            confMgr.addSubscription("wal", "LinkIndexUpdater", RowLogSubscription.Type.VM, 3, 10);
        }
        if (!confMgr.subscriptionExists("wal", "MQFeeder")) {
            confMgr.addSubscription("wal", "MQFeeder", RowLogSubscription.Type.VM, 3, 20);
        }

        messageQueue = new RowLogImpl("mq", HBaseTableUtil.getRecordTable(hbaseConf), RecordCf.MQ_PAYLOAD.bytes,
                RecordCf.MQ_STATE.bytes, 10000L, true, confMgr);
        messageQueue.registerShard(new RowLogShardImpl("shard1", hbaseConf, messageQueue, 100));

        writeAheadLog = new RowLogImpl("wal", HBaseTableUtil.getRecordTable(hbaseConf), RecordCf.WAL_PAYLOAD.bytes,
                RecordCf.WAL_STATE.bytes, 10000L, true, confMgr);
        RowLogShard walShard = new RowLogShardImpl("shard1", hbaseConf, writeAheadLog, 100);
        writeAheadLog.registerShard(walShard);

        RowLogMessageListenerMapping.INSTANCE.put("MQFeeder", new MessageQueueFeeder(messageQueue));

        // Start the message queue processor
        messageQueueProcessorLeader = new RowLogProcessorElection(zk, new RowLogProcessorImpl(messageQueue, confMgr));
        messageQueueProcessorLeader.start();
        
        // Start the wal processor
        writeAheadLogProcessorLeader = new RowLogProcessorElection(zk, new RowLogProcessorImpl(writeAheadLog, confMgr));
        writeAheadLogProcessorLeader.start();
        
        confMgr.addListener("wal", "LinkIndexUpdater", "LinkIndexUpdaterListener");
        confMgr.addListener("wal", "MQFeeder", "MQFeederListener");
    }

    @PreDestroy
    public void stop() throws RowLogException, InterruptedException, KeeperException {
        messageQueueProcessorLeader.stop();
        writeAheadLogProcessorLeader.stop();
        messageQueue.stop();
        writeAheadLog.stop();
        confMgr.removeListener("wal", "LinkIndexUpdater", "LinkIndexUpdaterListener");
        confMgr.removeListener("wal", "MQFeeder", "MQFeederListener");
        RowLogMessageListenerMapping.INSTANCE.remove("MQFeeder");        
    }

    public RowLog getMessageQueue() {
        return messageQueue;
    }

    public RowLog getWriteAheadLog() {
        return writeAheadLog;
    }
}
