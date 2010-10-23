package org.lilyproject.rest;

public class ResourceException extends RuntimeException {
    private int status;
    private String message;

    public ResourceException(String message, int status) {
        super(message);
        this.status = status;
        this.message = message;
    }

    public ResourceException(String message, Throwable cause, int status) {
        super(message, cause);
        this.status = status;
        this.message = message;
    }

    public ResourceException(Throwable cause, int status) {
        super(cause);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    /**
     * Gets the message specifically set on this exception. This is different
     * from {@link #getMessage()} which might inherit the message from a nested
     * throwable or so.
     */
    public String getSpecificMessage() {
        return message;
    }
}
