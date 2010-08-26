package org.lilycms.rest.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class RecordWriter {
    public static JsonNode toJson(Record record, TypeManager typeManager) throws FieldTypeNotFoundException, TypeException {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode recordNode = factory.objectNode();

        Namespaces namespaces = new Namespaces();

        recordNode.put("id", record.getId().toString());
        recordNode.put("type", typeToJson(record.getRecordTypeName(), record.getRecordTypeVersion(), namespaces));

        QName versionedTypeName = record.getRecordTypeName(Scope.VERSIONED);
        if (versionedTypeName != null) {
            long version = record.getRecordTypeVersion(Scope.VERSIONED);
            recordNode.put("versionedType", typeToJson(versionedTypeName, version, namespaces));
        }

        QName versionedMutableTypeName = record.getRecordTypeName(Scope.VERSIONED_MUTABLE);
        if (versionedMutableTypeName != null) {
            long version = record.getRecordTypeVersion(Scope.VERSIONED_MUTABLE);
            recordNode.put("versionedMutableType", typeToJson(versionedMutableTypeName, version, namespaces));
        }

        Map<QName, Object> fields = record.getFields();
        if (fields.size() > 0) {
            ObjectNode fieldsNode = recordNode.putObject("fields");
            ObjectNode schemaNode = recordNode.putObject("schema");

            for (Map.Entry<QName, Object> field : fields.entrySet()) {
                FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());
                String fieldName = QNameConverter.toJson(fieldType.getName(), namespaces);

                // fields entry
                fieldsNode.put(fieldName, valueToJson(field.getValue(), fieldType));

                // schema entry
                schemaNode.put(fieldName, FieldTypeWriter.toJson(fieldType, namespaces, false));
            }
        }

        if (record.getVersion() != null) {
            recordNode.put("version", record.getVersion());
        }

        recordNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return recordNode;
    }

    private static JsonNode valueToJson(Object value, FieldType fieldType) {
        if (fieldType.getValueType().isMultiValue()) {
            List list = (List)value;
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            for (Object item : list) {
                array.add(hierarchicalValueToJson(item, fieldType));
            }
            return array;
        } else {
            return hierarchicalValueToJson(value, fieldType);
        }
    }

    private static JsonNode hierarchicalValueToJson(Object value, FieldType fieldType) {
        if (fieldType.getValueType().isHierarchical()) {
            HierarchyPath path = (HierarchyPath)value;
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            for (Object element : path.getElements()) {
                array.add(primitiveValueToJson(element, fieldType));
            }
            return array;
        } else {
            return primitiveValueToJson(value, fieldType);
        }
    }

    private static JsonNode primitiveValueToJson(Object value, FieldType fieldType) {
        String type = fieldType.getValueType().getPrimitive().getName();

        JsonNodeFactory factory = JsonNodeFactory.instance;

        JsonNode result;

        if (type.equals("STRING")) {
            result = factory.textNode((String)value);
        } else if (type.equals("LONG")) {
            result = factory.numberNode((Long)value);
        } else if (type.equals("DOUBLE")) {
            result = factory.numberNode((Double)value);
        } else if (type.equals("BOOLEAN")) {
            result = factory.booleanNode((Boolean)value);
        } else if (type.equals("INTEGER")) {
            result = factory.numberNode((Integer)value);
        } else if (type.equals("URI") || type.equals("DATETIME") || type.equals("DATE") || type.equals("LINK")) {
            result = factory.textNode(value.toString());
        } else if (type.equals("DECIMAL")) {
            result = factory.numberNode((BigDecimal)value);
        } else if (type.equals("BLOB")) {
            Blob blob = (Blob)value;
            ObjectNode jsonBlob = factory.objectNode();
            jsonBlob.put("value", blob.getValue());
            jsonBlob.put("mimeType", blob.getMimetype());
            if (blob.getName() != null)
                jsonBlob.put("name", blob.getName());
            jsonBlob.put("size", blob.getSize());
            result = jsonBlob;
        } else {
            throw new RuntimeException("Unsupported primitive value type: " + type);
        }

        return result;
    }

    private static JsonNode typeToJson(QName name, long version, Namespaces namespaces) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode jsonType = factory.objectNode();

        jsonType.put("name", QNameConverter.toJson(name, namespaces));
        jsonType.put("version", version);

        return jsonType;
    }


}
