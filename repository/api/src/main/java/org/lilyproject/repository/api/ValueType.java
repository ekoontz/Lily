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
package org.lilyproject.repository.api;

/**
 * A value type represents the type of the value of a {@link FieldType}.
 *
 * <p>It consists of:
 * <ul>
 *  <li>A {@link PrimitiveValueType}: this is a particular kind of value like a string, long, ... It could
 *      also be a composite value, e.g. a coordinate with x and y values.
 *  <li>a multi-value flag: to indicate if the value should be multi-value, in which case the value is a
 *      java.util.List.
 *  <li>a hierarchical flag: to indicate if the value should be a hierarchical path, in which case the value is
 *      a {@link HierarchyPath}.
 * </ul>
 *
 * <p>The reason for having the separate concept of a {@link PrimitiveValueType} is so that we could have a multi-value
 * and/or hierarchical variant of any kind of value.
 *
 * <p>A field can be either multi-value or hierarchical, or both at the same time. In the last case, the value
 * is a java.util.List of {@link HierarchyPath} objects, not the other way round.
 *
 * <p>So you can have a multi-valued string, a multi-valued date, a single-valued hierarchical string path,
 * a multi-valued hierarchical string path, ...
 *
 * <p>It is the responsibility of a ValueType to convert the values to/from byte representation, as used for
 * storage in the repository. This should delegate to the PrimitiveValueType for the conversion of a single value.
 *
 * <p>It is not necessary to create value types in the repository, they simply exist for any kind of primitive
 * value type. You can get access to ValueType instances via {@link TypeManager#getValueType(String, boolean, boolean)}.
 *
 */
public interface ValueType {

    /**
     * Is this a multi-value value type.
     */
    boolean isMultiValue();
    
    /**
     * Is this a hierarchical value type. See also {@link HierarchyPath}.
     */
    boolean isHierarchical();

    /**
     * Is this a primitive value type. A value type is primitive if it is not hierarchical and not multi-value, thus
     * if its values directly correspond to the {@link #getPrimitive PrimitiveValueType}. This is a shortcut method
     * to avoid checking the other flags individually.
     */
    boolean isPrimitive();

    /**
     * Decodes a byte[] to an object of the type represented by this {@link ValueType}. See {@link ValueType#getType()} 
     */
    public Object fromBytes(byte[] value);

    /**
     * Encodes an object of the type represented by this {@link ValueType} to a byte[].
     */
    byte[] toBytes(Object value);

    /**
     * Gets the primitive value type.
     */
    PrimitiveValueType getPrimitive();
    
    /**
     * Returns the Java {@link Class} object for the values of this value type.
     *
     * <p>For multi-value fields (including those which are hierarchical), this will be java.util.List.
     * For single-valued hierarchical fields this is {@link HierarchyPath}. In all other cases, this is the same
     * as {@link PrimitiveValueType#getType}.
     */
    Class getType();

    /**
     * Returns an encoded byte[] representing this value type.
     */
    byte[] toBytes();
    
    boolean equals(Object obj);
}
