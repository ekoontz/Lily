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

import java.util.Arrays;

import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;

public class RowLogMessageImpl implements RowLogMessage {

    private final byte[] rowKey;
    private final long seqnr;
    private final byte[] data;
    private final RowLog rowLog;
    private final long timestamp;

    public RowLogMessageImpl(long timestamp, byte[] rowKey, long seqnr, byte[] data, RowLog rowLog) {
        this.timestamp = timestamp;
        this.rowKey = rowKey;
        this.seqnr = seqnr;
        this.data = data;
        this.rowLog = rowLog;
    }
    
    public byte[] getData() {
        return data;
    }

    public byte[] getPayload() throws RowLogException {
        return rowLog.getPayload(this);
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public long getSeqNr() {
        return seqnr;
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(rowKey);
        result = prime * result + (int) (seqnr ^ (seqnr >>> 32));
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowLogMessageImpl other = (RowLogMessageImpl) obj;
        if (!Arrays.equals(rowKey, other.rowKey))
            return false;
        if (seqnr != other.seqnr)
            return false;
        if (timestamp != other.timestamp)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowLogMessageImpl [rowLog=" + rowLog.getId() + ", timestamp=" + timestamp + ", rowKey="
                + Arrays.toString(rowKey) + ", seqnr=" + seqnr + "]";
    }    
    
    
}
