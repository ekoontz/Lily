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
 * Represents the primitive type that can be used for the value of a field. It should be embedded in a
 * {@link ValueType} before using it in a {@link FieldType}.
 *
 * <p>A primitive value type is responsible for the encoding to and decoding from {@code byte[]} of the values of their
 * type.
 *
 * <p>PrimitiveValueType's are obtained as part of a value type via
 * {@link TypeManager#getValueType(String, boolean, boolean) TypeManager.getValueType}.
 *
 * <p>There are a number of built-in primitive value types, they are listed at
 * {@link TypeManager#getValueType(String, boolean, boolean) TypeManager.getValueType}.
 *
 * <p>When a new primitive type should be made available, this interface needs to be implemented and registered by calling
 * {@link TypeManager#registerPrimitiveValueType(PrimitiveValueType) TypeManager.registerPrimitiveValueType}.
 */
public interface PrimitiveValueType {
    /**
     * The unique name of this primitive value type.
     */
    String getName();

    /**
     * Decodes a byte[] to an object of the actual type of this primitive value type.
     */
    public Object fromBytes(byte[] bytes);

    /**
     * Encodes an object of the actual type of this primitive value type to a byte[].
     */
    byte[] toBytes(Object value);

    /**
     * The Java type of the values of this primitive value type.
     */
    Class getType();
    
    @Override
    boolean equals(Object obj);
}
