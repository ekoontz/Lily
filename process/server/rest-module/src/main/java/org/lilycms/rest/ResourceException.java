package org.lilycms.rest;

public class ResourceException extends RuntimeException {
    private int status;

    public ResourceException(String message, int status) {
        super(message);
        this.status = status;
    }

    public ResourceException(String message, Throwable cause, int status) {
        super(message, cause);
        this.status = status;
    }

    public ResourceException(Throwable cause, int status) {
        super(cause);
        this.status = status;
    }
}
