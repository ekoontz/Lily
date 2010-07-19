package org.lilycms.repository.api;

/**
 * Generic exception for blob-related errors.
 */
public class BlobException extends RepositoryException {
    public BlobException() {
        super();
    }

    public BlobException(String message) {
        super(message);
    }

    public BlobException(String message, Throwable cause) {
        super(message, cause);
    }

    public BlobException(Throwable cause) {
        super(cause);
    }
}
