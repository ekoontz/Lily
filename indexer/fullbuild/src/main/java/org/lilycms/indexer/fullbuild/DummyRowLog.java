package org.lilycms.indexer.fullbuild;

import org.apache.hadoop.hbase.client.Put;
import org.lilycms.rowlog.api.*;

import java.util.List;

public class DummyRowLog implements RowLog {
    private final String failMessage;

    public DummyRowLog(String failMessage) {
        this.failMessage = failMessage;
    }

    public String getId() {
        throw new RuntimeException(failMessage);
    }

    public void registerShard(RowLogShard shard) {
        throw new RuntimeException(failMessage);
    }

    public void unRegisterShard(RowLogShard shard) {
        throw new RuntimeException(failMessage);
    }

    public byte[] getPayload(RowLogMessage message) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public boolean processMessage(RowLogMessage message) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public byte[] lockMessage(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public boolean unlockMessage(RowLogMessage message, String subscriptionId, boolean realTry, byte[] lock) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public boolean isMessageLocked(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public boolean messageDone(RowLogMessage message, String subscriptionId, byte[] lock) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public List<RowLogMessage> getMessages(byte[] rowKey, String... subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public List<RowLogMessage> getProblematic(String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public boolean isProblematic(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }

    public List<SubscriptionContext> getSubscriptions() {
        throw new RuntimeException(failMessage);
    }

    public List<RowLogShard> getShards() {
        throw new RuntimeException(failMessage);
    }

    public boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }
}
