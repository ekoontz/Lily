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
package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.VersionTag;

import static org.lilyproject.util.json.JsonUtil.*;

public class FieldTypeReader implements EntityReader<FieldType> {
    public static EntityReader<FieldType> INSTANCE = new FieldTypeReader();

    public FieldType fromJson(ObjectNode node, Repository repository) throws JsonFormatException, RepositoryException {
        Namespaces namespaces = NamespacesConverter.fromContextJson(node);
        return fromJson(node, namespaces, repository);
    }

    public FieldType fromJson(ObjectNode node, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException {

        QName name = QNameConverter.fromJson(getString(node, "name"), namespaces);

        JsonNode vtNode = getNode(node, "valueType");
        String primitive = getString(vtNode, "primitive");
        boolean multiValue = getBoolean(vtNode, "multiValue", false);
        boolean hierarchical = getBoolean(vtNode, "hierarchical", false);

        String scopeName = getString(node, "scope", "non_versioned");
        Scope scope = parseScope(scopeName);

        TypeManager typeManager = repository.getTypeManager();
        ValueType valueType = typeManager.getValueType(primitive, multiValue, hierarchical);
        FieldType fieldType = typeManager.newFieldType(valueType, name, scope);

        String id = getString(node, "id", null);
        fieldType.setId(id);

        // Some sanity checks for version tag fields
        if (fieldType.getName().getNamespace().equals(VersionTag.NAMESPACE)) {
            if (fieldType.getScope() != Scope.NON_VERSIONED)
                throw new JsonFormatException("vtag fields should be in the non-versioned scope");

            if (!fieldType.getValueType().getPrimitive().getName().equals("LONG"))
                throw new JsonFormatException("vtag fields should be of type LONG");

            if (fieldType.getValueType().isMultiValue())
                throw new JsonFormatException("vtag fields should not be multi-valued");

            if (fieldType.getValueType().isHierarchical())
                throw new JsonFormatException("vtag fields should not be hierarchical");
        }

        return fieldType;
    }

    private static Scope parseScope(String scopeName) {
        scopeName = scopeName.toLowerCase();
        if (scopeName.equals("non_versioned")) {
            return Scope.NON_VERSIONED;
        } else if (scopeName.equals("versioned")) {
            return Scope.VERSIONED;
        } else if (scopeName.equals("versioned_mutable")) {
            return Scope.VERSIONED_MUTABLE;
        } else {
            throw new RuntimeException("Unrecognized scope name: " + scopeName);
        }
    }
}
