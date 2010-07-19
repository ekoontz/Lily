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
 * Describes the association between a {@link RecordType} and a {@link FieldType}. Or in other words, the occurrence of a
 * field type within a record type.
 *
 * <p>At this time, the only attribute specific to this relation indicates if the field type is mandatory
 * within the record type.
 *
 * <p>Field type entries are added to record types via
 * {@link RecordType#addFieldTypeEntry(FieldTypeEntry) RecordType.addFieldTypeEntry} and instantiated via
 * {@link TypeManager#newFieldTypeEntry(String, boolean) TypeManager.newFieldTypeEntry}. As all entities within this
 * API, field type entries are dumb data objects.
 */
public interface FieldTypeEntry {

    void setFieldTypeId(String id);

    String getFieldTypeId();

    void setMandatory(boolean mandatory);

    boolean isMandatory();

    FieldTypeEntry clone();

    boolean equals(Object obj);
}
