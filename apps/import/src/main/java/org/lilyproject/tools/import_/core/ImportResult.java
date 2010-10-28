/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.import_.core;

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
