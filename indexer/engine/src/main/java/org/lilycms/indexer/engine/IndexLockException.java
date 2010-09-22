package org.lilycms.indexer.engine;

public class IndexLockException extends Exception {
    public IndexLockException(String message) {
        super(message);
    }

    public IndexLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
