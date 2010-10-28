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
package org.lilyproject.indexer.batchbuild;

import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.rowlog.api.*;

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

    public List<RowLogSubscription> getSubscriptions() {
        throw new RuntimeException(failMessage);
    }

    public List<RowLogShard> getShards() {
        throw new RuntimeException(failMessage);
    }

    public boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException {
        throw new RuntimeException(failMessage);
    }
}
