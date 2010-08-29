package org.lilycms.tools.import_.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;
import org.lilycms.util.repo.VersionTag;

import static org.lilycms.util.repo.JsonUtil.*;

public class FieldTypeReader {
    public static FieldType fromJson(ObjectNode node, TypeManager typeManager) throws JsonFormatException {
        Namespaces namespaces = NamespacesConverter.fromContextJson(node);
        return fromJson(node, namespaces, typeManager);
    }

    public static FieldType fromJson(ObjectNode node, Namespaces namespaces, TypeManager typeManager)
            throws JsonFormatException {

        QName name = QNameConverter.fromJson(getString(node, "name"), namespaces);

        JsonNode vtNode = getNode(node, "valueType");
        String primitive = getString(vtNode, "primitive");
        boolean multiValue = getBoolean(vtNode, "multiValue", false);
        boolean hierarchical = getBoolean(vtNode, "hierarchical", false);

        String scopeName = getString(node, "scope", "non_versioned");
        Scope scope = parseScope(scopeName);

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
