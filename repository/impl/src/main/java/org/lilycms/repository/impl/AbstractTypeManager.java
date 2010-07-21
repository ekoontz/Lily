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

import java.util.HashMap;
import java.util.Map;

import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.FieldTypeEntry;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.primitivevaluetype.*;
import org.lilycms.util.ArgumentValidator;

public abstract class AbstractTypeManager implements TypeManager {
    protected Map<String, PrimitiveValueType> primitiveValueTypes = new HashMap<String, PrimitiveValueType>();
    protected IdGenerator idGenerator;

    public RecordType newRecordType(String recordTypeId) {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");
        return new RecordTypeImpl(recordTypeId);
    }

    public FieldTypeEntry newFieldTypeEntry(String fieldTypeId, boolean mandatory) {
        ArgumentValidator.notNull(fieldTypeId, "fieldTypeId");
        ArgumentValidator.notNull(mandatory, "mandatory");
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }

    public FieldType newFieldType(ValueType valueType, QName name, Scope scope) {
        return newFieldType(null, valueType, name, scope);
    }

    public FieldType newFieldType(String id, ValueType valueType, QName name,
            Scope scope) {
                ArgumentValidator.notNull(valueType, "valueType");
                ArgumentValidator.notNull(name, "name");
                ArgumentValidator.notNull(scope, "scope");
                return new FieldTypeImpl(id, valueType, name, scope);
            }

    public void registerPrimitiveValueType(PrimitiveValueType primitiveValueType) {
        primitiveValueTypes.put(primitiveValueType.getName(), primitiveValueType);
    }

    public ValueType getValueType(String primitiveValueTypeName, boolean multivalue, boolean hierarchy) {
        PrimitiveValueType type = primitiveValueTypes.get(primitiveValueTypeName);
        if (type == null) {
            // TODO should probably be another kind of exception, this was just a quick fix to avoid NPE
            throw new IllegalArgumentException("Primitive value type does not exist: " + primitiveValueTypeName);
        }
        return new ValueTypeImpl(type, multivalue, hierarchy);
    }
    
    protected void initialize() {
        registerDefaultValueTypes();
    }
    
    // TODO get this from some configuration file
    protected void registerDefaultValueTypes() {
        //
        // Important:
        //
        // When adding a type below, please update the list of built-in
        // types in the javadoc of the method TypeManager.getValueType.
        //

        registerPrimitiveValueType(new StringValueType());
        registerPrimitiveValueType(new IntegerValueType());
        registerPrimitiveValueType(new LongValueType());
        registerPrimitiveValueType(new BooleanValueType());
        registerPrimitiveValueType(new DateValueType());
        registerPrimitiveValueType(new DateTimeValueType());
        registerPrimitiveValueType(new LinkValueType(idGenerator));
        registerPrimitiveValueType(new BlobValueType());
    }
}
