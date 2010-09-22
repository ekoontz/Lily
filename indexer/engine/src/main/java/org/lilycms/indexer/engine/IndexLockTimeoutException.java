package org.lilycms.indexer.engine;

public class IndexLockTimeoutException extends IndexLockException {
    public IndexLockTimeoutException(String message) {
        super(message);
    }
}
