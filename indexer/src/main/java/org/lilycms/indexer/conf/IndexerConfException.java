package org.lilycms.indexer.conf;

/**
 * Thrown when there is an error in the user-provided configuration.
 */
public class IndexerConfException extends Exception {
    public IndexerConfException() {
        super();
    }

    public IndexerConfException(String message) {
        super(getMessage(message));
    }

    public IndexerConfException(String message, Throwable cause) {
        super(getMessage(message), cause);
    }

    public IndexerConfException(Throwable cause) {
        super(cause);
    }

    private static String getMessage(String message) {
        return "Indexer configuration: " + message;
    }
}
