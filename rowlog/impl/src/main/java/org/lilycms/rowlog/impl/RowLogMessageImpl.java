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
package org.lilycms.rowlog.impl;

import java.util.Arrays;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;

public class RowLogMessageImpl implements RowLogMessage {

    private final byte[] rowKey;
    private final long seqnr;
    private final byte[] data;
    private final byte[] id;
    private final RowLog rowLog;

    public RowLogMessageImpl(byte[] id, byte[] rowKey, long seqnr, byte[] data, RowLog rowLog) {
        this.id = id;
        this.rowKey = rowKey;
        this.seqnr = seqnr;
        this.data = data;
        this.rowLog = rowLog;
    }
    
    public byte[] getId() {
        return id;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(data);
        result = prime * result + Arrays.hashCode(id);
        result = prime * result + Arrays.hashCode(rowKey);
        result = prime * result + (int) (seqnr ^ (seqnr >>> 32));
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
        if (!Arrays.equals(data, other.data))
            return false;
        if (!Arrays.equals(id, other.id))
            return false;
        if (!Arrays.equals(rowKey, other.rowKey))
            return false;
        if (seqnr != other.seqnr)
            return false;
        return true;
    }
    
}
