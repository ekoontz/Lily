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
package org.lilyproject.rowlog.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.rowlog.avro.AvroExecState;
import org.lilyproject.rowlog.avro.AvroExecStateEntry;

public class SubscriptionExecutionState {

    private final long timestamp;

    private final Map<CharSequence, AvroExecStateEntry> entries = new HashMap<CharSequence, AvroExecStateEntry>();

    private static final short FORMAT_VERSION = 1;

    public SubscriptionExecutionState(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    public AvroExecStateEntry getEntry(String subscriptionId) {
        AvroExecStateEntry entry = entries.get(subscriptionId);
        if (entry == null) {
            entry = new AvroExecStateEntry();
            entry.done = false;
            entry.tryCount = 0;
            entries.put(subscriptionId, entry);
        }
        return entry;
    }

    public Set<CharSequence> getSubscriptionIds() {
        return entries.keySet();
    }

    public void setState(String subscriptionId, boolean state) {
        getEntry(subscriptionId).done = state;
    }
    
    public boolean getState(String subscriptionId) {
        AvroExecStateEntry entry = entries.get(subscriptionId);
        if (entry != null) {
            return entry.done;
        } else {
            return true;
        }
    }
    
    public void incTryCount(String subscriptionId) {
        AvroExecStateEntry entry = getEntry(subscriptionId);
        entry.tryCount = entry.tryCount + 1;
    }
    
    public void decTryCount(String subscriptionId) {
        AvroExecStateEntry entry = getEntry(subscriptionId);
        if (entry.tryCount <= 0) {
            entry.tryCount = 0;
        } else {
            entry.tryCount = entry.tryCount - 1;
        }
    }
    
    public int getTryCount(String subscriptionId) {
        AvroExecStateEntry entry = entries.get(subscriptionId);
        if (entry != null) {
            return entry.tryCount;
        } else {
            return 0;
        }
    }
    
    public void setLock(String subscriptionId, byte[] lock) {
        getEntry(subscriptionId).lock = lock == null ? null : ByteBuffer.wrap(lock);
    }
    
    public byte[] getLock(String subscriptionId) {
        AvroExecStateEntry entry = entries.get(subscriptionId);
        if (entry != null) {
            return entry.lock == null ? null : entry.lock.array();
        } else {
            return null;
        }
    }

    private static final SpecificDatumWriter<AvroExecState> STATE_WRITER =
            new SpecificDatumWriter<AvroExecState>(AvroExecState.class);

    private static final SpecificDatumReader<AvroExecState> STATE_READER =
            new SpecificDatumReader<AvroExecState>(AvroExecState.class);

    public byte[] toBytes() {
        AvroExecState state = new AvroExecState();
        state.timestamp = timestamp;
        state.entries = entries;

        ByteArrayOutputStream os = new ByteArrayOutputStream(400);
        BinaryEncoder encoder = new BinaryEncoder(os);
        try {
            // First write a version number to support future evolution of the serialization format
            os.write(Bytes.toBytes(FORMAT_VERSION));

            STATE_WRITER.write(state, encoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return os.toByteArray();
    }

    private void setEntries(Map<CharSequence, AvroExecStateEntry> entries) {
        // The reason for copying over the entries, rather than simply assigning the full Map, is that
        // the Map created by Avro will contain Utf8 objects as key.
        for (Map.Entry<CharSequence, AvroExecStateEntry> entry : entries.entrySet()) {
            this.entries.put(entry.getKey().toString(), entry.getValue());
        }
    }

    public static SubscriptionExecutionState fromBytes(byte[] bytes) throws IOException {
        short version = Bytes.toShort(bytes, 0, 2);

        if (version != FORMAT_VERSION) {
            throw new RuntimeException("Unsupported subscription execution state serialized format version: " + version);
        }

        AvroExecState state = STATE_READER.read(null,
                DecoderFactory.defaultFactory().createBinaryDecoder(bytes, 2, bytes.length - 2, null));

        SubscriptionExecutionState result = new SubscriptionExecutionState(state.timestamp);

        result.setEntries(state.entries);

        return result;
    }

    public boolean allDone() {
        for (AvroExecStateEntry entry : entries.values()) {
            if (!entry.done)
                return false;
        }
        return true;
    }
}
