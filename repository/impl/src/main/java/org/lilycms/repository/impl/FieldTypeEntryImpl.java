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

import org.lilycms.repository.api.FieldTypeEntry;
import org.lilycms.util.ArgumentValidator;

/**
 *
 */
public class FieldTypeEntryImpl implements FieldTypeEntry {

    private String fieldTypeId;
    private boolean mandatory;

    public FieldTypeEntryImpl(String fieldTypeId, boolean mandatory) {
        ArgumentValidator.notNull(fieldTypeId, "fieldTypeId");
        this.fieldTypeId = fieldTypeId;
        this.mandatory = mandatory;
    }

    public String getFieldTypeId() {
        return fieldTypeId;
    }
    
    public boolean isMandatory() {
        return mandatory;
    }

    public void setFieldTypeId(String id) {
        this.fieldTypeId = id;
    }
    
    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }
    
    public FieldTypeEntry clone() {
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldTypeId == null) ? 0 : fieldTypeId.hashCode());
        result = prime * result + (mandatory ? 1231 : 1237);
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
        FieldTypeEntryImpl other = (FieldTypeEntryImpl) obj;
        if (fieldTypeId == null) {
            if (other.fieldTypeId != null)
                return false;
        } else if (!fieldTypeId.equals(other.fieldTypeId))
            return false;
        if (mandatory != other.mandatory)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FieldTypeEntryImpl [fieldTypeId=" + fieldTypeId + ", mandatory=" + mandatory + "]";
    }

}
