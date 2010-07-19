package org.lilycms.repository.api;

public class RepositoryRuntimeException extends RuntimeException {
    public RepositoryRuntimeException() {
        super();
    }

    public RepositoryRuntimeException(String message) {
        super(message);
    }

    public RepositoryRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RepositoryRuntimeException(Throwable cause) {
        super(cause);
    }
}
