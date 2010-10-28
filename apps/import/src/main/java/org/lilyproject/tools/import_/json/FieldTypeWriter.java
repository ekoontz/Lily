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

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.ValueType;

public class FieldTypeWriter implements EntityWriter<FieldType> {
    public static EntityWriter<FieldType> INSTANCE = new FieldTypeWriter();

    public ObjectNode toJson(FieldType fieldType, Repository repository) {
        Namespaces namespaces = new Namespaces();

        ObjectNode fieldNode = toJson(fieldType, namespaces, repository);

        fieldNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return fieldNode;
    }

    public ObjectNode toJson(FieldType fieldType, Namespaces namespaces, Repository repository) {
        return toJson(fieldType, namespaces, true);
    }

    public static ObjectNode toJson(FieldType fieldType, Namespaces namespaces, boolean includeName) {
        ObjectNode fieldNode = JsonNodeFactory.instance.objectNode();

        fieldNode.put("id", fieldType.getId());

        if (includeName) {
            fieldNode.put("name", QNameConverter.toJson(fieldType.getName(), namespaces));
        }

        fieldNode.put("scope", fieldType.getScope().toString().toLowerCase());

        fieldNode.put("valueType", valueTypeToJson(fieldType.getValueType()));

        return fieldNode;
    }

    public static ObjectNode valueTypeToJson(ValueType valueType) {
        ObjectNode vtNode = JsonNodeFactory.instance.objectNode();

        vtNode.put("primitive", valueType.getPrimitive().getName());
        vtNode.put("multiValue", valueType.isMultiValue());
        vtNode.put("hierarchical", valueType.isHierarchical());

        return vtNode;
    }
}
