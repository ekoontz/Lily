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
package org.lilycms.repository.api;

import java.util.List;
import java.util.Map;

import org.lilycms.repository.api.FieldNotFoundException;

/**
 * A Record is the core entity managed by the {@link Repository}.
 *
 * <p>A Record can be instantiated via {@link Repository#newRecord() Repository.newRecord} or retrieved via
 * {@link Repository#read(RecordId) Repository.read}. As all entities within this API, records are dumb data objects.
 *
 * <p>All Record-related CRUD operations are available on the {@link Repository} interface:
 * {@link Repository#create(Record) Create}, {@link Repository#read(RecordId) Read},
 * {@link Repository#update(Record) Update}, {@link Repository#delete(RecordId) Delete}.
 *
 * <p>A Record object is not necessarily a representation of a complete record.
 * When {@link Repository#read(RecordId, java.util.List) reading}
 * a record, you can specify to only read certain fields. Likewise, when {@link Repository#update(Record) updating},
 * you only need to put the fields in this object that you want to be updated. But it is not necessary to remove
 * unchanged fields from this object, the repository will compare with the current situation and ignore
 * unchanged fields.
 *
 * <p>Since for an update, this object only needs to contain the fields you want to update, fields that are
 * not in this object will not be automatically removed from the record. Rather, you have to say explicitly which
 * fields should be deleted by adding them to the {@link #getFieldsToDelete() fields-to-delete} list. If the
 * fields-to-delete list contains field names that do not exist in the record, then these will be ignored upon
 * update, rather than causing an exception.
 * 
 * <p>The {@link RecordType} and its version define the schema of the record. As is explained in more detail in
 * Lily's repository model documentation, a record has a pointer to three (possibly) different record types, one
 * for each scope.
 * 
 */
public interface Record {
    void setId(RecordId recordId);

    RecordId getId();

    void setVersion(Long version);

    /**
     * Returns the version.
     *
     * <p>For a record without versions, this returns null. In all other cases, this returns the version number
     * of the loaded version (= the latest one by default), even if none of the versioned fields would actually be
     * loaded.
     */
    Long getVersion();

    /**
     * Sets the record type and record type version.
     * 
     * <p>This actually sets the record type of the non-versioned scope, which is considered to be the primary
     * record type. Upon save, the record types of the other scopes will also be set to this record type (if there
     * are any fields changed in those scopes, thus if a new version will be created).
     * 
     * @param version version number, or null if you want the repository to pick the last version available when
     *                storing the record.  
     */
    void setRecordType(QName name, Long version);
    
    /**
     * Shortcut for setRecordType(name, null)
     */
    void setRecordType(QName name);

    /**
     * Returns the record type of the non-versioned scope.
     */
    QName getRecordTypeName();

    /**
     * Returns the record type version of the non-versioned scope.
     */
    Long getRecordTypeVersion();
    
    /**
     * Sets the record type of a specific scope
     */
    void setRecordType(Scope scope, QName name, Long version);
    
    /**
     * Returns the record type of a specific scope
     */
    QName getRecordTypeName(Scope scope);
    
    /**
     * Returns the record type version of a specific scope
     */
    Long getRecordTypeVersion(Scope scope);

    /**
     * Sets a field to a value, possibly replacing a previous value.
     *
     * <p>The provided QName should be the QName of {@link FieldType} that exists within the repository.
     *
     * <p>The type of the given value should correspond to the
     * {@link ValueType#getClass() value type} of the {@link FieldType#getValueType() field type}.
     */
    void setField(QName fieldName, Object value);

    /**
     * Deletes a field from this object and optionally adds it to the list of fields to delete upon save.
     *
     * <p>If the field is not present, this does not throw an exception. In this case, the field will still
     * be added to the list of fields to delete. Use {@link #hasField} if you want to check if a field is
     * present in this Record instance.
     *
     * @param addToFieldsToDelete if false, the field will only be removed from this value object, but not be added
     *                            to the {@link #getFieldsToDelete}.
     */
    void delete(QName fieldName, boolean addToFieldsToDelete);

    /**
     * Gets a field, throws an exception if it is not present.
     *
     * <p>To do anything useful with the returned value, you have to cast it to the type you expect.
     * See also {@link ValueType}.
     *
     * @throws FieldNotFoundException if the field is not present in this Record object. To avoid the exception,
     *         use {@link #hasField}.
     */
    Object getField(QName fieldName) throws FieldNotFoundException;

    /**
     * Checks if the field with the given name is present in this Record object.
     */
    boolean hasField(QName fieldName);

    /**
     * Returns the map of fields in this Record.
     *
     * <p>Important: changing this map or the values contained within them will alter the content of this Record.
     */
    Map<QName, Object> getFields();

    /**
     * Adds the given fields to the list of fields to delete.
     */
    void addFieldsToDelete(List<QName> fieldNames);
    
    /**
     * Removes the given fields to the list of fields to delete.
     */
    void removeFieldsToDelete(List<QName> fieldNames);

    /**
     * Gets the lists of fields to delete. Modifying the returned list modifies the state of this Record object.
     */
    List<QName> getFieldsToDelete();
    
    Record clone();
    
    boolean equals(Object obj);

    /**
     * Compares the two records, ignoring some aspects. This method is intended to compare
     * a record loaded from the repository with a newly instantiated one in which some things
     * are typically not supplied.
     *
     * <p>The aspects which are ignored are:
     * <ul>
     *   <li>the version
     *   <li>the record types: only the name of the non-versioned record type is compared, if
     *       it is not null in both. The version of the record type is not compared.
     * </ul>
     */
    boolean softEquals(Object obj);
}
