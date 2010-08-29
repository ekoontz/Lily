package org.lilycms.rest.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilycms.repository.api.*;
import org.lilycms.util.repo.JsonUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.lilycms.util.repo.JsonUtil.*;

public class RecordReader {
    private Namespaces namespaces;
    private Repository repository;

    /**
     * When you need to read a lot of records using the same namespace context and repository,
     * the same RecordReader instance can be reused.
     */
    public RecordReader(Namespaces namespaces, Repository repository) {
        this.namespaces = namespaces;
        this.repository = repository;
    }

    public static Record fromJson(ObjectNode recordNode, Repository repository)
            throws JsonFormatException, RepositoryException {
        Namespaces namespaces = NamespacesConverter.fromContextJson(recordNode);
        return new RecordReader(namespaces, repository).readRecord(recordNode);
    }

    public static Record readRecord(ObjectNode recordNode, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException {
        return new RecordReader(namespaces, repository).readRecord(recordNode);
    }

    public Record readRecord(ObjectNode recordNode) throws JsonFormatException, RepositoryException {
        Record record = repository.newRecord();

        String id = getString(recordNode, "id", null);
        if (id != null) {
            record.setId(repository.getIdGenerator().newRecordId(id));
        }

        // TODO support versioned type: {name: "", version: ...}
        String type = getString(recordNode, "type", null);
        if (type != null)
            record.setRecordType(QNameConverter.fromJson(type, namespaces));

        ObjectNode fields = getObject(recordNode, "fields");
        Iterator<Map.Entry<String, JsonNode>> it = fields.getFields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();

            QName qname = QNameConverter.fromJson(entry.getKey(), namespaces);
            FieldType fieldType = repository.getTypeManager().getFieldTypeByName(qname);
            Object value = readMultiValue(fields.get(entry.getKey()), fieldType, entry.getKey());
            record.setField(qname, value);
        }

        return record;
    }

    private Object readMultiValue(JsonNode node, FieldType fieldType, String prop) throws JsonFormatException {
        if (fieldType.getValueType().isMultiValue()) {
            if (!node.isArray()) {
                throw new JsonFormatException("Multi-value value should be specified as array in " + prop);
            }

            List<Object> value = new ArrayList<Object>();
            for (int i = 0; i < node.size(); i++) {
                value.add(readHierarchical(node.get(i), fieldType, prop));
            }

            return value;
        } else {
            return readHierarchical(node, fieldType, prop);
        }
    }

    private Object readHierarchical(JsonNode node, FieldType fieldType, String prop) throws JsonFormatException {
        if (fieldType.getValueType().isHierarchical()) {
            if (!node.isArray()) {
                throw new JsonFormatException("Hierarchical value should be specified as an array in " + prop);
            }

            Object[] elements = new Object[node.size()];
            for (int i = 0; i < node.size(); i++) {
                elements[i] = readPrimitive(node.get(i), fieldType, prop);
            }

            return new HierarchyPath(elements);
        } else {
            return readPrimitive(node, fieldType, prop);
        }
    }

    private Object readPrimitive(JsonNode node, FieldType fieldType, String prop) throws JsonFormatException {
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

            String mimeType = JsonUtil.getString(blobNode, "mimeType", null);
            long size = JsonUtil.getLong(blobNode, "size");
            String name = JsonUtil.getString(blobNode, "name", null);
            byte[] value = JsonUtil.getBinary(blobNode, "value");

            Blob blob = new Blob(mimeType, size, name);
            blob.setValue(value);

            return blob;
        } else {
            throw new JsonFormatException("Primitive value type not supported: " + primitive);
        }
    }
}
