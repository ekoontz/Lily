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

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.kauriproject.conf.Conf;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.rowlog.impl.MessageQueueFeeder;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogProcessorElection;
import org.lilyproject.rowlog.impl.RowLogProcessorImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class RowLogSetup {
    private final RowLogConfigurationManager confMgr;
    private final Configuration hbaseConf;
    private final ZooKeeperItf zk;
    private RowLogImpl messageQueue;
    private RowLogImpl writeAheadLog;
    private RowLogProcessorElection messageQueueProcessorLeader;
    private RowLogProcessorElection writeAheadLogProcessorLeader;
    private final HBaseTableFactory hbaseTableFactory;
    private final Conf rowLogConf;
    private final Log log = LogFactory.getLog(getClass());

    public RowLogSetup(RowLogConfigurationManager confMgr, ZooKeeperItf zk, Configuration hbaseConf,
            HBaseTableFactory hbaseTableFactory, Conf rowLogConf) {
        this.confMgr = confMgr;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.hbaseTableFactory = hbaseTableFactory;
        this.rowLogConf = rowLogConf;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, LeaderElectionSetupException, RowLogException {
        if (!confMgr.rowLogExists("wal")) {
            confMgr.addRowLog("wal", new RowLogConfig(10000L, true, false, 100L, 5000L));
        }
        
        if (!confMgr.rowLogExists("mq")) {
            confMgr.addRowLog("mq", new RowLogConfig(10000L, true, true, 100L, 0L));
        }
        
        // If the subscription already exists, this method will silently return
        if (!confMgr.subscriptionExists("wal", "LinkIndexUpdater")) {
            confMgr.addSubscription("wal", "LinkIndexUpdater", RowLogSubscription.Type.VM, 3, 10);
        }
        if (!confMgr.subscriptionExists("wal", "MQFeeder")) {
            confMgr.addSubscription("wal", "MQFeeder", RowLogSubscription.Type.VM, 3, 20);
        }

        messageQueue = new RowLogImpl("mq", hbaseTableFactory.getRecordTable(), RecordCf.MQ_PAYLOAD.bytes,
                RecordCf.MQ_STATE.bytes, confMgr);
        messageQueue.registerShard(new RowLogShardImpl("shard1", hbaseConf, messageQueue, 100));

        writeAheadLog = new RowLogImpl("wal", hbaseTableFactory.getRecordTable(), RecordCf.WAL_PAYLOAD.bytes,
                RecordCf.WAL_STATE.bytes, confMgr);
        RowLogShard walShard = new RowLogShardImpl("shard1", hbaseConf, writeAheadLog, 100);
        writeAheadLog.registerShard(walShard);

        RowLogMessageListenerMapping.INSTANCE.put("MQFeeder", new MessageQueueFeeder(messageQueue));

        // Start the message queue processor
        boolean mqProcEnabled = rowLogConf.getChild("mqProcessor").getAttributeAsBoolean("enabled", true);
        if (mqProcEnabled) {
            messageQueueProcessorLeader = new RowLogProcessorElection(zk, new RowLogProcessorImpl(messageQueue, confMgr));
            messageQueueProcessorLeader.start();
        } else {
            log.info("Not participating in MQ processor election.");
        }
        
        // Start the wal processor
        boolean walProcEnabled = rowLogConf.getChild("walProcessor").getAttributeAsBoolean("enabled", true);
        if (walProcEnabled) {
            writeAheadLogProcessorLeader = new RowLogProcessorElection(zk, new RowLogProcessorImpl(writeAheadLog, confMgr));
            writeAheadLogProcessorLeader.start();
        } else {
            log.info("Not participating in WAL processor election.");
        }
        
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
