package org.lilycms.server.modules.rowlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.lilycms.rowlog.api.*;
import org.lilycms.rowlog.impl.*;
import org.lilycms.util.hbase.HBaseTableUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

public class RowLogSetup {
    private final RowLogConfigurationManager confMgr;
    private final Configuration hbaseConf;
    private final ZooKeeperItf zk;
    private RowLog messageQueue;
    private RowLog writeAheadLog;
    private RowLogProcessorImpl messageQueueProcessor;

    public RowLogSetup(RowLogConfigurationManager confMgr, ZooKeeperItf zk, Configuration hbaseConf) {
        this.confMgr = confMgr;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, RowLogException {
        // If the subscription already exists, this method will silently return
        confMgr.addSubscription("WAL", "LinkIndexUpdater", SubscriptionContext.Type.VM, 3, 10);
        confMgr.addSubscription("WAL", "MQFeeder", SubscriptionContext.Type.VM, 3, 20);

        messageQueue = new RowLogImpl("MQ", HBaseTableUtil.getRecordTable(hbaseConf), HBaseTableUtil.MQ_PAYLOAD_COLUMN_FAMILY,
                HBaseTableUtil.MQ_COLUMN_FAMILY, 10000L, true, zk);
        messageQueue.registerShard(new RowLogShardImpl("MQS1", hbaseConf, messageQueue, 100));

        writeAheadLog = new RowLogImpl("WAL", HBaseTableUtil.getRecordTable(hbaseConf), HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY,
                HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, true, zk);
        RowLogShard walShard = new RowLogShardImpl("WS1", hbaseConf, writeAheadLog, 100);
        writeAheadLog.registerShard(walShard);

        RowLogMessageListenerMapping listenerClassMapping = RowLogMessageListenerMapping.INSTANCE;
        listenerClassMapping.put("MQFeeder", new MessageQueueFeeder(messageQueue));
        listenerClassMapping.put("LinkIndexUpdater", new RowLogMessageListener() {
            public boolean processMessage(RowLogMessage message) {
                // TODO
                return true;
            }
        });


        // Start the processor
        messageQueueProcessor = new RowLogProcessorImpl(messageQueue, zk);
        messageQueueProcessor.start();
    }

    @PreDestroy
    public void stop() {
        messageQueueProcessor.stop();
    }

    public RowLog getMessageQueue() {
        return messageQueue;
    }

    public RowLog getWriteAheadLog() {
        return writeAheadLog;
    }
}
