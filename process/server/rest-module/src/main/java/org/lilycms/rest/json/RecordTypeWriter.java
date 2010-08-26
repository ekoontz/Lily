package org.lilycms.rest.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.FieldTypeEntry;
import org.lilycms.repository.api.RecordType;

public class RecordTypeWriter {
    public static ObjectNode toJson(RecordType recordType) {
        Namespaces namespaces = new Namespaces();

        ObjectNode rtNode = toJson(recordType, namespaces);

        rtNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return rtNode;
    }

    public static ObjectNode toJson(RecordType recordType, Namespaces namespaces) {
        return toJson(recordType, namespaces, true);
    }

    public static ObjectNode toJson(RecordType recordType, Namespaces namespaces, boolean includeName) {
        ObjectNode rtNode = JsonNodeFactory.instance.objectNode();

        rtNode.put("id", recordType.getId());

        if (includeName) {
            rtNode.put("name", QNameConverter.toJson(recordType.getName(), namespaces));
        }

        ArrayNode fieldsNode = rtNode.putArray("fields");
        for (FieldTypeEntry entry : recordType.getFieldTypeEntries()) {
            ObjectNode entryNode = fieldsNode.addObject();
            entryNode.put("id", entry.getFieldTypeId());
            entryNode.put("mandatory", entry.isMandatory());
        }

        return rtNode;
    }

}
