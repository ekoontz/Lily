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

/**
 * TypeManager provides access to the repository schema. This is where {@link RecordType}s and {@link FieldType}s
 * are managed.
 *
 * <p>For an in-depth description of the repository model, please see the Lily documentation.
 */
public interface TypeManager {
    
    /**
     * Instantiates a new RecordType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    RecordType newRecordType(String recordTypeId);

    /**
     * Creates a RecordType in the repository.
     *
     * @throws RecordTypeExistsException when a recordType with the same id already exists on the repository 
     * @throws RecordTypeNotFoundException when a mixin of the recordType refers to a non-existing {@link RecordType} 
     * @throws FieldTypeNotFoundException 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, TypeException;
    
    /**
     * Gets a RecordType from the repository.
     *
     * @param version the version of the record type to return, or null for the latest version.
     *
     * @throws RecordTypeNotFoundException when the recordType does not exist
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType getRecordType(String id, Long version) throws RecordTypeNotFoundException, TypeException;

    /**
     * Updates an existing record type.
     *
     * <p>You can provide any RecordType object as argument, either retrieved via {@link #getRecordType(String, Long)} or
     * newly instantiated via {@link #newRecordType(String)}.
     *
     * <p>The state of the record type will be updated to correspond to the given RecordType object. This also
     * concerns the list of fields: any fields that were previously in the record type but are not present in
     * the provided RecordType object will be removed. This is different from {@link Record}s, where field deletion
     * is explicit.
     *
     * <p>Upon each update, a new version of the RecordType is created. The number of the created version is available
     * from the returned RecordType object.
     *
     * @throws RecordTypeNotFoundException when the recordType to be updated does not exist
     * @throws FieldTypeNotFoundException 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    RecordType updateRecordType(RecordType recordType) throws  RecordTypeNotFoundException, FieldTypeNotFoundException,
            TypeException;

    /**
     * Instantiates a new FieldTypeEntry object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     *
     * <p>FieldTypeEntries can be added to {@link RecordType}s.
     */
    FieldTypeEntry newFieldTypeEntry(String fieldTypeId, boolean mandatory);
    
    /**
     * Instantiates a new FieldType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    FieldType newFieldType(ValueType valueType, QName name, Scope scope);
    
    /**
     * Instantiates a new FieldType object.
     *
     * <p>This is only a factory method, nothing is created in the repository.
     */
    FieldType newFieldType(String id, ValueType valueType, QName name, Scope scope);
    
    /**
     * Creates a FieldType in the repository.
     *
     * <p>The ID of a field type is assigned by the system. If there is an ID present in the provided FieldType
     * object, it will be ignored. The generated ID is available from the returned FieldType object.
     *
     * @return updated FieldType object
     *
     * @throws RepositoryException when an unexpected exception occurs on the repository
     * @throws FieldTypeExistsException 
     */
    FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, TypeException;

    /**
     * Updates an existing FieldType.
     *
     * <p>You can provide any FieldType object as argument, either retrieved via {@link #getFieldTypeByName} or
     * newly instantiated via {@link #newFieldType}.
     *
     * <p>It is the ID of the field type which serves to identify the field type, so the ID must be present in the
     * FieldType object. The QName of the field type can be changed.
     *
     * @return updated FieldType object
     *
     * @throws FieldTypeNotFoundException when no fieldType with id and version exists
     * @throws FieldTypeUpdateException an exception occurred while updating the FieldType 
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType updateFieldType(FieldType fieldType) throws FieldTypeNotFoundException, FieldTypeUpdateException,
            TypeException;
    
    /**
     * Gets a FieldType from the repository.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given ID exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException, TypeException;

    /**
     * Gets a FieldType from the repository.
     *
     * @throws FieldTypeNotFoundException when no fieldType with the given name exists
     * @throws RepositoryException when an unexpected exception occurs on the repository
     */
    FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException, TypeException;

    /**
     * Provides {@link ValueType} instances. These are used to set to value type of {@link FieldType}s.
     *
     * <p>The built-in available primitive value types are listed in the following table.
     *
     * <table>
     * <tbody>
     * <tr><th>Name</th>     <th>Class</th></tr>
     * <tr><td>STRING</td>   <td>java.lang.String</td></tr>
     * <tr><td>INTEGER</td>  <td>java.lang.Integer</td></tr>
     * <tr><td>LONG</td>     <td>java.lang.Long</td></tr>
     * <tr><td>DOUBLE</td>   <td>java.lang.Double</td></tr>
     * <tr><td>DECIMAL</td>   <td>java.math.BigDecimal</td></tr>
     * <tr><td>BOOLEAN</td>  <td>java.lang.Boolean</td></tr>
     * <tr><td>DATE</td>     <td>org.joda.time.LocalDate</td></tr>
     * <tr><td>DATETIME</td> <td>org.joda.time.DateTime</td></tr>
     * <tr><td>BLOB</td>     <td>org.lilycms.repository.api.Blob</td></tr>
     * <tr><td>LINK</td>     <td>org.lilycms.repository.api.Link</td></tr>
     * <tr><td>URI</td>      <td>java.net.URI</td></tr>
     * </tbody>
     * </table>
     *
     * @param primitiveValueTypeName the name of the {@link PrimitiveValueType} to be encapsulated by this
     *                               {@link ValueType}. See table above.
     * @param multiValue if this {@link ValueType} should represent a multi value field or not
     * @param hierarchical if this{@link ValueType} should represent a {@link HierarchyPath} field or not
     */
    ValueType getValueType(String primitiveValueTypeName, boolean multiValue, boolean hierarchical);

    /**
     * Registers custom {@link PrimitiveValueType}s.
     *
     * <p><b>TODO:</b> Maybe this should rather move to an SPI interface? Can this replace a built-in primitive
     * value type if the name corresponds? Does it make sense to allow registering at any time? Probably implies
     * registering on all Lily nodes? This needs more thought.
     */
    void registerPrimitiveValueType(PrimitiveValueType primitiveValueType);
}
