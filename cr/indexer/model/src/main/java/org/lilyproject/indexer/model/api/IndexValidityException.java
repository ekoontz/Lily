package org.lilyproject.indexer.model.api;

public class IndexValidityException extends Exception {
    public IndexValidityException(String message) {
        super(message);
    }

    public IndexValidityException(String message, Throwable cause) {
        super(message, cause);
    }
}
