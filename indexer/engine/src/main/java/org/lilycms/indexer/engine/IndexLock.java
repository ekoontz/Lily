package org.lilycms.indexer.engine;

import org.lilycms.repository.api.RecordId;

import java.util.Arrays;

public class IndexLock {
    private RecordId recordId;
    private byte[] token;

    protected IndexLock(RecordId recordId, byte[] token) {
        this.recordId = recordId;
        this.token = token;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        IndexLock other = (IndexLock)obj;
        return other.recordId.equals(recordId) && Arrays.equals(other.token, token);
    }
}
