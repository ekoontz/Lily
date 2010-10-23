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
package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.ValueType;

import java.util.List;
import java.util.Set;

/**
 * Formats field values to string for transfer to SOLR.
 *
 * <p>Concerning the supports* methods: you should have at least one of singleValue
 * or multiValue return true, and at least one of nonHierarchical or hierarchical return
 * true. Thus if your formatter can only format simple plain values, you should have
 * both supportsSingleValue() and supportsNonHierarchicalValue() return true.
 */
public interface Formatter {
    List<String> format(Object value, ValueType valueType);

    /**
     * Returning an empty set means this formatter accepts any kind of value (even non-built-in
     * value types one might not know about when implementing the formatter).
     */
    Set<String> getSupportedPrimitiveValueTypes();

    /**
     * Returns true if this formatter can format single (= non-multi-value) values.
     */
    boolean supportsSingleValue();

    /**
     * Returns true if this formatter can format multi-valued values.
     */
    boolean supportsMultiValue();

    /**
     * Returns true if this formatter can format non-hierarchical values.
     */
    boolean supportsNonHierarchicalValue();

    /**
     * Returns true if this formatter can format hierarchical values.
     */
    boolean supportsHierarchicalValue();
}
