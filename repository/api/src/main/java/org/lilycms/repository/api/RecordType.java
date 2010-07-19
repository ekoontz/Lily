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

import java.util.Collection;
import java.util.Map;

/**
 * A record type describes the schema to be followed by a {@link Record}.
 *
 * <p>Record types are managed via the {@link TypeManager}. To instantiate a RecordType use
 * {@link TypeManager#newRecordType(String) TypeManager.newRecordType}. As all entities within this API,
 * record types are dumb data objects.
 *
 * <p>A record type consists of:
 *
 * <ul>
 * <li>a list of field types, associated via {@link FieldTypeEntry} which defines properties specific to the use
 * of a field type within this record type.
 * <li>a list of mixins, these are references to other record types to be mixed in (imported within) this record
 * type.
 * </ul>
 *
 * <p>Record types are versioned: upon each update, a new version of the record type is created. Record store a
 * pointer to the particular version of a record type that was used when creating/updating a record type. The references
 * to the mixin record types are also to specific versions.
 */
public interface RecordType {
    String getId();

    void setVersion(Long version);
    
    Long getVersion();

    /**
     * Adds a field type entry. A field type entry can be instantiated via {@link TypeManager#newFieldTypeEntry(String, boolean)}.
     */
    void addFieldTypeEntry(FieldTypeEntry fieldTypeEntry);

    /**
     * A shortcut for adding a field type entry without having to instantiate it yourself.
     */
    FieldTypeEntry addFieldTypeEntry(String fieldTypeId, boolean mandatory);

    FieldTypeEntry getFieldTypeEntry(String fieldTypeId);
    
    void removeFieldTypeEntry(String fieldTypeId);
    
    Collection<FieldTypeEntry> getFieldTypeEntries();
    
    void addMixin(String recordTypeId, Long recordTypeVersion);
    
    void removeMixin(String recordTypeId);
    
    Map<String, Long> getMixins();
    
    RecordType clone();
    
    boolean equals(Object obj);
}
