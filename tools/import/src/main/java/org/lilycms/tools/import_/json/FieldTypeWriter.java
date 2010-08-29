package org.lilycms.tools.import_.json;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.ValueType;

public class FieldTypeWriter {
    public static ObjectNode toJson(FieldType fieldType) {
        Namespaces namespaces = new Namespaces();

        ObjectNode fieldNode = toJson(fieldType, namespaces);

        fieldNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return fieldNode;
    }

    public static ObjectNode toJson(FieldType fieldType, Namespaces namespaces) {
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
