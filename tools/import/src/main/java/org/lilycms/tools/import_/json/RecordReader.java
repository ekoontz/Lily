package org.lilycms.tools.import_.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilycms.repository.api.*;
import org.lilycms.util.json.JsonUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.lilycms.util.json.JsonUtil.*;

public class RecordReader implements EntityReader<Record> {
    public static EntityReader<Record> INSTANCE = new RecordReader();

    public Record fromJson(ObjectNode node, Repository repository) throws JsonFormatException, RepositoryException {
        Namespaces namespaces = NamespacesConverter.fromContextJson(node);
        return fromJson(node, namespaces, repository);
    }

    public Record fromJson(ObjectNode node, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException {

        Record record = repository.newRecord();

        String id = getString(node, "id", null);
        if (id != null) {
            record.setId(repository.getIdGenerator().newRecordId(id));
        }

        JsonNode typeNode = node.get("type");
        if (typeNode != null) {
            if (typeNode.isObject()) {
                QName qname = QNameConverter.fromJson(JsonUtil.getString(typeNode, "name"), namespaces);
                Long version = JsonUtil.getLong(typeNode, "version", null);
                record.setRecordType(qname, version);
            } else if (typeNode.isTextual()) {
                record.setRecordType(QNameConverter.fromJson(typeNode.getTextValue(), namespaces));
            }
        }

        ObjectNode fields = getObject(node, "fields", null);
        if (fields != null) {
            Iterator<Map.Entry<String, JsonNode>> it = fields.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();

                QName qname = QNameConverter.fromJson(entry.getKey(), namespaces);
                FieldType fieldType = repository.getTypeManager().getFieldTypeByName(qname);
                Object value = readMultiValue(fields.get(entry.getKey()), fieldType, entry.getKey(), repository);
                record.setField(qname, value);
            }
        }

        ArrayNode fieldsToDelete = getArray(node, "fieldsToDelete", null);
        if (fieldsToDelete != null) {
            for (int i = 0; i < fieldsToDelete.size(); i++) {
                JsonNode fieldToDelete = fieldsToDelete.get(i);
                if (!fieldToDelete.isTextual()) {
                    throw new JsonFormatException("fieldsToDelete should be an array of strings, encountered: " + fieldToDelete);
                } else {
                    QName qname = QNameConverter.fromJson(fieldToDelete.getTextValue(), namespaces);
                    record.getFieldsToDelete().add(qname);
                }
            }
        }

        return record;
    }

    private Object readMultiValue(JsonNode node, FieldType fieldType, String prop, Repository repository)
            throws JsonFormatException {

        if (fieldType.getValueType().isMultiValue()) {
            if (!node.isArray()) {
                throw new JsonFormatException("Multi-value value should be specified as array in " + prop);
            }

            List<Object> value = new ArrayList<Object>();
            for (int i = 0; i < node.size(); i++) {
                value.add(readHierarchical(node.get(i), fieldType, prop, repository));
            }

            return value;
        } else {
            return readHierarchical(node, fieldType, prop, repository);
        }
    }

    private Object readHierarchical(JsonNode node, FieldType fieldType, String prop, Repository repository)
            throws JsonFormatException {

        if (fieldType.getValueType().isHierarchical()) {
            if (!node.isArray()) {
                throw new JsonFormatException("Hierarchical value should be specified as an array in " + prop);
            }

            Object[] elements = new Object[node.size()];
            for (int i = 0; i < node.size(); i++) {
                elements[i] = readPrimitive(node.get(i), fieldType, prop, repository);
            }

            return new HierarchyPath(elements);
        } else {
            return readPrimitive(node, fieldType, prop, repository);
        }
    }

    private Object readPrimitive(JsonNode node, FieldType fieldType, String prop, Repository repository)
            throws JsonFormatException {

        String primitive = fieldType.getValueType().getPrimitive().getName();

        if (primitive.equals("STRING")) {
            if (!node.isTextual())
                throw new JsonFormatException("Expected text value for " + prop);

            return node.getTextValue();
        } else if (primitive.equals("INTEGER")) {
            if (!node.isIntegralNumber())
                throw new JsonFormatException("Expected int value for " + prop);

            return node.getIntValue();
        } else if (primitive.equals("LONG")) {
            if (!node.isIntegralNumber())
                throw new JsonFormatException("Expected long value for " + prop);

            return node.getLongValue();
        } else if (primitive.equals("DOUBLE")) {
            if (!node.isNumber())
                throw new JsonFormatException("Expected double value for " + prop);

            return node.getDoubleValue();
        } else if (primitive.equals("DECIMAL")) {
            if (!node.isNumber())
                throw new JsonFormatException("Expected decimal value for " + prop);

            return node.getDecimalValue();
        } else if (primitive.equals("URI")) {
            if (!node.isTextual())
                throw new JsonFormatException("Expected URI (string) value for " + prop);

            try {
                return new URI(node.getTextValue());
            } catch (URISyntaxException e) {
                throw new JsonFormatException("Invalid URI in property " + prop + ": " + node.getTextValue());
            }
        } else if (primitive.equals("BOOLEAN")) {
            if (!node.isBoolean())
                throw new JsonFormatException("Expected boolean value for " + prop);

            return node.getBooleanValue();
        } else if (primitive.equals("LINK")) {
            if (!node.isTextual())
                throw new JsonFormatException("Expected text value for " + prop);

            return Link.fromString(node.getTextValue(), repository.getIdGenerator());
        } else if (primitive.equals("DATE")) {
            if (!node.isTextual())
                throw new JsonFormatException("Expected text value for " + prop);

            return new LocalDate(node.getTextValue());
        } else if (primitive.equals("DATETIME")) {
            if (!node.isTextual())
                throw new JsonFormatException("Expected text value for " + prop);

            return new DateTime(node.getTextValue());
        } else if (primitive.equals("BLOB")) {
            if (!node.isObject())
                throw new JsonFormatException("Expected object value for " + prop);

            ObjectNode blobNode = (ObjectNode)node;
            return BlobConverter.fromJson(blobNode);
        } else {
            throw new JsonFormatException("Primitive value type not supported: " + primitive);
        }
    }
}
