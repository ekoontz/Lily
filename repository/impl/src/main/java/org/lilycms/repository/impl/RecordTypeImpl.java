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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.lilycms.repository.api.FieldTypeEntry;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.util.ArgumentValidator;

public class RecordTypeImpl implements RecordType {
    
    private final String id;
    private Long version;
    private Map<String, Long> mixins = new HashMap<String, Long>();
    private Map<String, FieldTypeEntry> fieldTypeEntries = new HashMap<String, FieldTypeEntry>();

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version){
        this.version = version;
    }
    
    public Collection<FieldTypeEntry> getFieldTypeEntries() {
        return fieldTypeEntries.values();
    }
    
    public FieldTypeEntry getFieldTypeEntry(String fieldTypeId) {
        return fieldTypeEntries.get(fieldTypeId);
    }
    
    public void removeFieldTypeEntry(String fieldTypeId) {
        fieldTypeEntries.remove(fieldTypeId);
    }
    
    public void addFieldTypeEntry(FieldTypeEntry fieldTypeEntry) {
        fieldTypeEntries.put(fieldTypeEntry.getFieldTypeId(), fieldTypeEntry);
    }

    public FieldTypeEntry addFieldTypeEntry(String fieldTypeId, boolean mandatory) {
        FieldTypeEntry fieldTypeEntry = new FieldTypeEntryImpl(fieldTypeId, mandatory);
        addFieldTypeEntry(fieldTypeEntry);
        return fieldTypeEntry;
    }

    public void addMixin(String recordTypeId, Long recordTypeVersion) {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");
        mixins.put(recordTypeId, recordTypeVersion);
    }
    
    public void addMixin(String recordTypeId) {
        addMixin(recordTypeId, null);
    }
    
    public void removeMixin(String recordTypeId) {
        mixins.remove(recordTypeId);
    }
    
    public Map<String, Long> getMixins() {
        return mixins;
    }

    public RecordType clone() {
        RecordTypeImpl clone = new RecordTypeImpl(this.id);
        clone.version = this.version;
        clone.fieldTypeEntries.putAll(fieldTypeEntries);
        clone.mixins.putAll(mixins);
        return clone;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldTypeEntries == null) ? 0 : fieldTypeEntries.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((mixins == null) ? 0 : mixins.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecordTypeImpl other = (RecordTypeImpl) obj;
        if (fieldTypeEntries == null) {
            if (other.fieldTypeEntries != null)
                return false;
        } else if (!fieldTypeEntries.equals(other.fieldTypeEntries))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (mixins == null) {
            if (other.mixins != null)
                return false;
        } else if (!mixins.equals(other.mixins))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RecordTypeImpl [id=" + id + ", version=" + version + ", fieldTypeEntries=" + fieldTypeEntries.values()
                        + ", mixins=" + mixins + "]";
    }


}
