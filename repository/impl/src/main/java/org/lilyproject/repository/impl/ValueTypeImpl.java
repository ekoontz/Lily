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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.PrimitiveValueType;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.util.ArgumentValidator;

public class ValueTypeImpl implements ValueType {

    private final PrimitiveValueType primitiveValueType;
    private final boolean multiValue;
    private final boolean hierarchical;

    public ValueTypeImpl(PrimitiveValueType primitiveValueType, boolean multivalue, boolean hierarchical) {
        ArgumentValidator.notNull(primitiveValueType, "primitiveValueType");
        this.primitiveValueType = primitiveValueType;
        this.multiValue = multivalue;
        this.hierarchical = hierarchical;
    }

    public boolean isMultiValue() {
        return multiValue;
    }
    
    public boolean isHierarchical() {
        return hierarchical;
    }

    public boolean isPrimitive() {
        return !multiValue && !hierarchical;
    }

    public Object fromBytes(byte[] bytes) {
        if (isMultiValue()) {
            return fromMultiValueBytes(bytes);
        } else if (isHierarchical()) {
            return fromHierarchicalBytes(bytes);
        } else {
            return primitiveValueType.fromBytes(bytes);
        }
    }

    private List fromMultiValueBytes(byte[] bytes) {
        List result = new ArrayList();
        int offset = 0;
        while (offset < bytes.length) {
            int valueLenght = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
            offset = offset + Bytes.SIZEOF_INT;
            byte[] valueBytes = new byte[valueLenght];
            Bytes.putBytes(valueBytes, 0, bytes, offset, valueLenght);
            Object value;
            if (isHierarchical()) {
                value = fromHierarchicalBytes(valueBytes);
            } else {
                value = primitiveValueType.fromBytes(valueBytes);
            }
            result.add(value);
            offset = offset + valueLenght;
        }
        return result;
    }
    
    private HierarchyPath fromHierarchicalBytes(byte[] bytes) {
        List<Object> result = new ArrayList<Object>();
        int offset = 0;
        while (offset < bytes.length) {
            int valueLenght = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
            offset = offset + Bytes.SIZEOF_INT;
            byte[] valueBytes = new byte[valueLenght];
            Bytes.putBytes(valueBytes, 0, bytes, offset, valueLenght);
            Object value = primitiveValueType.fromBytes(valueBytes);
            result.add(value);
            offset = offset + valueLenght;
        }
        return new HierarchyPath(result.toArray(new Object[result.size()]));
    }
    
    public byte[] toBytes(Object value) {
        if (isMultiValue()) {
            return toMultiValueBytes(value);
        } else if (isHierarchical()) {
            return toHierarchyBytes(value);
        } else {
            return primitiveValueType.toBytes(value);
        }
    }

    private byte[] toMultiValueBytes(Object value) {
        byte[] result;
        result = new byte[0];
        for(Object element : ((List<Object>)value)) {
            byte[] encodedValue;
            if (isHierarchical()) {
                encodedValue = toHierarchyBytes(element);
            } else {
                encodedValue = primitiveValueType.toBytes(element);
            }
            result = Bytes.add(result, Bytes.toBytes(encodedValue.length));
            result = Bytes.add(result, encodedValue);
        }
        return result;
    }
    
    private byte[] toHierarchyBytes(Object value) {
        byte[] result;
        result = new byte[0];
        for(Object element : ((HierarchyPath)value).getElements()) {
            byte[] encodedValue = primitiveValueType.toBytes(element);
            result = Bytes.add(result, Bytes.toBytes(encodedValue.length));
            result = Bytes.add(result, encodedValue);
        }
        return result;
    }

    public PrimitiveValueType getPrimitive() {
        return primitiveValueType;
    }
    
    public Class getType() {
        if (isMultiValue()) {
            return List.class;
        }
        return primitiveValueType.getType();
    }
    
    public byte[] toBytes() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(primitiveValueType.getName());
        stringBuilder.append(",");
        stringBuilder.append(Boolean.toString(isMultiValue()));
        stringBuilder.append(",");
        stringBuilder.append(Boolean.toString(isHierarchical()));
        return Bytes.toBytes(stringBuilder.toString());
    }

    public static ValueType fromBytes(byte[] bytes, AbstractTypeManager typeManager) {
        String encodedString = Bytes.toString(bytes);
        int endOfPrimitiveValueTypeName = encodedString.indexOf(",");
        String primitiveValueTypeName = encodedString.substring(0, endOfPrimitiveValueTypeName);
        int endOfMultiValueBoolean = encodedString.indexOf(",", endOfPrimitiveValueTypeName+1); 
        boolean multiValue = Boolean.parseBoolean(encodedString.substring(endOfPrimitiveValueTypeName + 1, endOfMultiValueBoolean));
        boolean hierarchical = Boolean.parseBoolean(encodedString.substring(endOfMultiValueBoolean + 1));
        return typeManager.getValueType(primitiveValueTypeName, multiValue, hierarchical);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (multiValue ? 1231 : 1237);
        result = prime * result + ((primitiveValueType == null) ? 0 : primitiveValueType.hashCode());
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
        ValueTypeImpl other = (ValueTypeImpl) obj;
        if (multiValue != other.multiValue)
            return false;
        if (primitiveValueType == null) {
            if (other.primitiveValueType != null)
                return false;
        } else if (!primitiveValueType.equals(other.primitiveValueType))
            return false;
        return true;
    }

}
