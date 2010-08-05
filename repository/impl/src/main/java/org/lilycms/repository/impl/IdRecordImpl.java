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
package org.lilycms.repository.impl;

import org.lilycms.repository.api.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IdRecordImpl implements IdRecord {
    private Record record;
    private Map<String, QName> mapping;
    private Map<Scope, String> recordTypeIds;

    public IdRecordImpl(Record record, Map<String, QName> idToQNameMapping, Map<Scope, String> recordTypeIds) {
        this.record = record;
        this.mapping = idToQNameMapping;
        this.recordTypeIds = recordTypeIds;
    }

    public Object getField(String fieldId) throws FieldNotFoundException {
        QName qname = mapping.get(fieldId);
        if (qname == null) {
            throw new FieldNotFoundException(fieldId);
        }
        return record.getField(qname);
    }

    public boolean hasField(String fieldId) {
        QName qname = mapping.get(fieldId);
        if (qname == null) {
            return false;
        }
        // Normally, we will only have a mapping for fields which are actually in the record,
        // but just to be sure:
        return record.hasField(qname);
    }

    public Map<String, Object> getFieldsById() {
        Map<QName, Object> fields = record.getFields();
        Map<String, Object> fieldsById = new HashMap<String, Object>(fields.size());

        for (Map.Entry<String, QName> entry : mapping.entrySet()) {
            Object value = fields.get(entry.getValue());
            if (value != null) {
                fieldsById.put(entry.getKey(), value);
            }
        }

        return fieldsById;
    }

    public String getRecordTypeId() {
        return recordTypeIds.get(Scope.NON_VERSIONED);
    }

    public String getRecordTypeId(Scope scope) {
        return recordTypeIds.get(scope);
    }

    public Record getRecord() {
        return record;
    }

    public void setId(RecordId recordId) {
        record.setId(recordId);
    }

    public RecordId getId() {
        return record.getId();
    }

    public void setVersion(Long version) {
        record.setVersion(version);
    }

    public Long getVersion() {
        return record.getVersion();
    }
    
    public void setRecordType(QName name, Long version) {
        record.setRecordType(name, version);
    }

    public void setRecordType(QName name) {
        record.setRecordType(name);
    }

    public QName getRecordTypeName() {
        return record.getRecordTypeName();
    }

    public Long getRecordTypeVersion() {
        return record.getRecordTypeVersion();
    }

    public void setRecordType(Scope scope, QName name, Long version) {
        record.setRecordType(scope, name, version);
    }

    public QName getRecordTypeName(Scope scope) {
        return record.getRecordTypeName(scope);
    }

    public Long getRecordTypeVersion(Scope scope) {
        return record.getRecordTypeVersion(scope);
    }

    public void setField(QName fieldName, Object value) {
        record.setField(fieldName, value);
    }

    public Object getField(QName fieldName) throws FieldNotFoundException {
        return record.getField(fieldName);
    }

    public boolean hasField(QName fieldName) {
        return record.hasField(fieldName);
    }

    public Map<QName, Object> getFields() {
        return record.getFields();
    }

    public void addFieldsToDelete(List<QName> fieldNames) {
        record.addFieldsToDelete(fieldNames);
    }

    public void removeFieldsToDelete(List<QName> fieldNames) {
        record.removeFieldsToDelete(fieldNames);
    }

    public List<QName> getFieldsToDelete() {
        return record.getFieldsToDelete();
    }

    public void delete(QName fieldName, boolean addToFieldsToDelete) {
        record.delete(fieldName, addToFieldsToDelete);
    }

    public Record clone() {
        throw new UnsupportedOperationException("IdRecordImpl does not support cloning.");
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("IdRecordImpl does not support equals.");
    }

    public boolean softEquals(Object obj) {
        throw new UnsupportedOperationException("IdRecordImpl does not support softEquals.");
    }

    
}
