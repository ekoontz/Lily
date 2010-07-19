package org.lilycms.repository.api;

/**
 * Generic exception for problems occurring during creating, reading, updating or
 * deleting field types or record types.
 */
public class TypeException extends RepositoryException {
    public TypeException() {
        super();
    }

    public TypeException(String message) {
        super(message);
    }

    public TypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TypeException(Throwable cause) {
        super(cause);
    }
}
