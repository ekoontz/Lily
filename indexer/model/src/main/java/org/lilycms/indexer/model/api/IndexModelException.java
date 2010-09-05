package org.lilycms.indexer.model.api;

public class IndexModelException extends Exception {
    public IndexModelException() {
        super();
    }

    public IndexModelException(String message) {
        super(message);
    }

    public IndexModelException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexModelException(Throwable cause) {
        super(cause);
    }
}
