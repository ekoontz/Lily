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
package org.lilycms.indexer.model.indexerconf;

import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.ValueType;

public interface Value {
    /**
     * Returns the field that is used from the record when evaluating this value. It is the value that is taken
     * from the current record, thus in the case of a dereference it is the first link field, not the field value
     * taken from the target document.
     */
    String getFieldDependency();

    /**
     * Name of the formatter to use for this value, or null if no specific one.
     */
    String getFormatter();

    boolean extractContent();

    /**
     * Returns the value type of the actual value to index. This does not necessarily correspond
     * to the value type of the field type returned by {@link #getTargetFieldType()} since it
     * might be 'corrected' to multi-value in case of multi-value link field dereferencing.
     */
    public abstract ValueType getValueType();

    /**
     * Get the FieldType of the field from which the actual data is taken, thus in case
     * of a dereference the last field in the chain.
     */
    public abstract FieldType getTargetFieldType();
}
