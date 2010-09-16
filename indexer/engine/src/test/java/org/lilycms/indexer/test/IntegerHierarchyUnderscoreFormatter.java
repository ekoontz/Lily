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
package org.lilycms.indexer.test;

import org.lilycms.indexer.model.indexerconf.Formatter;
import org.lilycms.repository.api.HierarchyPath;
import org.lilycms.repository.api.ValueType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class IntegerHierarchyUnderscoreFormatter implements Formatter {
    private static final Set<String> types = Collections.singleton("INTEGER");

    public List<String> format(Object value, ValueType valueType) {
        HierarchyPath path = (HierarchyPath)value;

        StringBuilder builder = new StringBuilder();
        for (Object item : path.getElements()) {
            if (builder.length() > 0)
                builder.append("_");
            builder.append(item);
        }

        return Collections.singletonList(builder.toString());
    }

    public Set<String> getSupportedPrimitiveValueTypes() {
        return types;
    }

    public boolean supportsSingleValue() {
        return true;
    }

    public boolean supportsMultiValue() {
        return false;
    }

    public boolean supportsNonHierarchicalValue() {
        return false;
    }

    public boolean supportsHierarchicalValue() {
        return true;
    }
}
