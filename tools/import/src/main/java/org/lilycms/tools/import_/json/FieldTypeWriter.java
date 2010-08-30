package org.lilycms.tools.import_.json;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.ValueType;

public class FieldTypeWriter implements EntityWriter<FieldType> {
    public static EntityWriter<FieldType> INSTANCE = new FieldTypeWriter();

    public ObjectNode toJson(FieldType fieldType, Repository repository) throws RepositoryException {
        Namespaces namespaces = new Namespaces();

        ObjectNode fieldNode = toJson(fieldType, namespaces, repository);

        fieldNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return fieldNode;
    }

    public ObjectNode toJson(FieldType fieldType, Namespaces namespaces, Repository repository) throws RepositoryException {
        return toJson(fieldType, namespaces, true);
    }

    public static ObjectNode toJson(FieldType fieldType, Namespaces namespaces, boolean includeName) throws RepositoryException {
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
