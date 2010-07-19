package org.lilycms.repository.api;

/**
 * Generic exception for problems occurring during creating, reading, updating or
 * deleting records.
 */
public class RecordException extends RepositoryException {
    public RecordException() {
        super();
    }

    public RecordException(String message) {
        super(message);
    }

    public RecordException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordException(Throwable cause) {
        super(cause);
    }
}
