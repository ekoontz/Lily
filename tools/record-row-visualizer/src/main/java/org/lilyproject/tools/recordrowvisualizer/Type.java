package org.lilyproject.tools.recordrowvisualizer;

/**
 * For describing information about a record type or a field type.
 */
public class Type<T> {
    protected String id;
    protected Long version;
    protected T object;

    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    public T getObject() {
        return object;
    }
}
