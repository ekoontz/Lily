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

import org.lilycms.repository.api.QName;

import java.util.Map;
import java.util.Set;

public class IndexCase {
    private final QName recordTypeName;
    /**
     * The variant properties the record should have. Evaluation rules: a key named
     * "*" (star symbol) is a wildcard meaning that any variant dimensions not specified
     * are accepted. Otherwise the variant dimension count should match exactly. The other
     * keys in the map are required variant dimensions. If their value is not null, the
     * values should match.
     */
    private final Map<String, String> variantPropsPattern;
    private final Set<String> vtags;
    private final boolean indexVersionless;

    public IndexCase(QName recordTypeName, Map<String, String> variantPropsPattern, Set<String> vtags,
            boolean indexVersionless) {
        this.recordTypeName = recordTypeName;
        this.variantPropsPattern = variantPropsPattern;
        this.vtags = vtags;
        this.indexVersionless = indexVersionless;
    }

    public boolean match(QName recordTypeName, Map<String, String> varProps) {
        if (!this.recordTypeName.equals(recordTypeName))
            return false;

        if (variantPropsPattern.size() != varProps.size() && !variantPropsPattern.containsKey("*")) {
            return false;
        }

        for (Map.Entry<String, String> entry : variantPropsPattern.entrySet()) {
            if (entry.getKey().equals("*"))
                continue;

            String dimVal = varProps.get(entry.getKey());
            if (dimVal == null) {
                // this record does not have a required variant property
                return false;
            }

            if (entry.getValue() != null && !entry.getValue().equals(dimVal)) {
                // the variant property does not have the required value
                return false;
            }
        }

        return true;
    }

    /**
     * Version tags identified by ID.
     */
    public Set<String> getVersionTags() {
        return vtags;
    }

    public boolean getIndexVersionless() {
        return indexVersionless;
    }
}
