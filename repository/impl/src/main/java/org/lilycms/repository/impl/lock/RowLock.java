package org.lilycms.repository.impl.lock;

public class RowLock {

    private final byte[] rowKey;
    private final long timestamp;

    public RowLock(byte[] rowKey, long timestamp) {
        this.rowKey = rowKey;
        this.timestamp = timestamp;
    }
    
    public byte[] getRowKey() {
        return rowKey;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
}
