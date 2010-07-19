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
 * A field type defines a field within a {@link Record}. Each field added to a record should be defined as a field
 * type.
 *
 * <p>A field type exists on its own (separate from any RecordType), but is typically associated with one or
 * more {@link RecordType}s.
 *
 * <p>Field types are managed via the {@link TypeManager}. To instantiate a field type use
 * {@link TypeManager#newFieldType(ValueType, QName, Scope) TypeManager.newFieldType}. As all entities within this
 * API, field types are dumb data objects.
 *
 * <p>A field type has two unique identifiers:
 * <ul>
 * <li>a system-generated id, immutable after creation of the field type
 * <li>a name in the form of a {@link QName qualified (namespaced) name}, which is mutable after creation of the field
 * type
 * </ul>
 *
 * <p>Each field type has a scope, the fields of a field type are said to belong to that scope. Scopes are
 * explained in detail in the Lily documentation.
 *
 * <p>In contrast with {@link RecordType}s, field types are not versioned. In fact, field types are almost immutable,
 * only their name can be changed after creation.
 */
public interface FieldType {

    /**
     * Sets the id.
     *
     * <p>Even though IDs are system-generated, you might need to set them on the field type e.g. to construct
     * a field type to pass to the {@link TypeManager#updateFieldType(FieldType)}.
     */
    void setId(String id);

    /**
     * The id is unique, immutable and system-generated.
     */
    String getId();

    void setName(QName name);

    /**
     * The name is unique, user-provided but can be changed after initial creation of the field type.
     */
    QName getName();

    /**
     * Sets the value type. Value type instances can be obtained from {@link TypeManager#getValueType}.
     */
    void setValueType(ValueType valueType);

    ValueType getValueType();

    void setScope(Scope scope);

    Scope getScope();

    boolean equals(Object obj);

    FieldType clone();
}
