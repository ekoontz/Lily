package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class RecordWriter implements EntityWriter<Record> {
    public static RecordWriter INSTANCE = new RecordWriter();

    public ObjectNode toJson(Record record, Repository repository) throws RepositoryException {
        Namespaces namespaces = new Namespaces();

        ObjectNode recordNode = toJson(record, namespaces, repository);

        recordNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return recordNode;
    }

    public ObjectNode toJson(Record record, Namespaces namespaces, Repository repository) throws RepositoryException {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode recordNode = factory.objectNode();

        recordNode.put("id", record.getId().toString());

        if (record.getVersion() != null) {
            recordNode.put("version", record.getVersion());
        }

        if (record.getRecordTypeName() != null) {
            recordNode.put("type", typeToJson(record.getRecordTypeName(), record.getRecordTypeVersion(), namespaces));
        }

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
                FieldType fieldType = repository.getTypeManager().getFieldTypeByName(field.getKey());
                String fieldName = QNameConverter.toJson(fieldType.getName(), namespaces);

                // fields entry
                fieldsNode.put(fieldName, valueToJson(field.getValue(), fieldType));

                // schema entry
                schemaNode.put(fieldName, FieldTypeWriter.toJson(fieldType, namespaces, false));
            }
        }

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
            result = BlobConverter.toJson(blob);
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
