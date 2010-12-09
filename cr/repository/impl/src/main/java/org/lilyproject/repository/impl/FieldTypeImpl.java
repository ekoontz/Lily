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
package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;

public class FieldTypeImpl implements FieldType {

    private String id;
    private byte[] idBytes;
    private ValueType valueType;
    private QName name;
    private Scope scope;

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newFieldType} instead
     */
    public FieldTypeImpl(String id, ValueType valueType, QName name, Scope scope) {
        this.id = id;
        this.valueType = valueType;
        this.name = name;
        this.scope = scope;
        setIdBytes(id);
    }

    private FieldTypeImpl(){}
    
    public QName getName() {
        return name;
    }

    public String getId() {
        return id;
    }
    
    protected byte[] getIdBytes() {
        return idBytes;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public Scope getScope() {
        return scope;
    }

    public void setId(String id) {
        this.id = id;
        setIdBytes(id);
    }
    
    private void setIdBytes(String id) {
        if (id == null)
            this.idBytes = null;
        else 
            this.idBytes = HBaseTypeManager.idToBytes(id);
    }
    
    public void setName(QName name) {
        this.name = name;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }
    
    public FieldType clone() {
        FieldTypeImpl newFieldType = new FieldTypeImpl();
        newFieldType.id = this.id;
        newFieldType.idBytes = this.idBytes;
        newFieldType.valueType = this.valueType;
        newFieldType.name = this.name;
        newFieldType.scope = this.scope;
        return newFieldType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
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
        FieldTypeImpl other = (FieldTypeImpl) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (valueType == null) {
            if (other.valueType != null)
                return false;
        } else if (!valueType.equals(other.valueType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FieldTypeImpl [id=" + id + ", name=" + name
                        + ", valueType=" + valueType + "]";
    }
}
