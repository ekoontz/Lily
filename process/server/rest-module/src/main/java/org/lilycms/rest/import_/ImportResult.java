package org.lilycms.rest.import_;

public class ImportResult<T> {
    private T entity;
    private ImportResultType resultType;
    private String conflictingProperty;
    private Object conflictingOldValue;
    private Object conflictingNewValue;

    public static <T> ImportResult<T> conflict(String property, Object oldValue, Object newValue) {
        ImportResult<T> result = new ImportResult<T>();
        result.resultType = ImportResultType.CONFLICT;
        result.conflictingProperty = property;
        result.conflictingOldValue = oldValue;
        result.conflictingNewValue = newValue;
        return result;
    }

    public static <T> ImportResult<T> created(T entity) {
        ImportResult<T> result = new ImportResult<T>();
        result.resultType = ImportResultType.CREATED;
        result.entity = entity;
        return result;
    }

    public static <T> ImportResult<T> updated(T entity) {
        ImportResult<T> result = new ImportResult<T>();
        result.resultType = ImportResultType.UPDATED;
        result.entity = entity;
        return result;
    }

    public static <T> ImportResult<T> upToDate(T entity) {
        ImportResult<T> result = new ImportResult<T>();
        result.resultType = ImportResultType.UP_TO_DATE;
        result.entity = entity;
        return result;
    }

    public static <T> ImportResult<T> cannotUpdateDoesNotExist() {
        ImportResult<T> result = new ImportResult<T>();
        result.resultType = ImportResultType.CANNOT_UPDATE_DOES_NOT_EXIST;
        return result;
    }

    public static <T> ImportResult<T> cannotCreateExists() {
        ImportResult<T> result = new ImportResult<T>();
        result.resultType = ImportResultType.CANNOT_CREATE_EXISTS;
        return result;
    }

    public T getEntity() {
        return entity;
    }

    public ImportResultType getResultType() {
        return resultType;
    }

    public String getConflictingProperty() {
        return conflictingProperty;
    }

    public Object getConflictingOldValue() {
        return conflictingOldValue;
    }

    public Object getConflictingNewValue() {
        return conflictingNewValue;
    }
}
