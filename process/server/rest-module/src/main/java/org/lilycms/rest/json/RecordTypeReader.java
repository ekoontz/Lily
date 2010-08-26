package org.lilycms.rest.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;

import static org.lilycms.util.repo.JsonUtil.*;

public class RecordTypeReader {
    public static RecordType fromJson(ObjectNode node, TypeManager typeManager) throws JsonFormatException {
        Namespaces namespaces = NamespacesConverter.fromContextJson(node);
        return fromJson(node, namespaces, typeManager);
    }

    public static RecordType fromJson(ObjectNode node, Namespaces namespaces, TypeManager typeManager)
            throws JsonFormatException {

        QName name = QNameConverter.fromJson(getString(node, "name"), namespaces);

        RecordType recordType = typeManager.newRecordType(name);

        String id = getString(node, "id", null);
        if (id != null)
            recordType.setId(id);

        JsonNode fields = getArray(node, "fields");
        for (int j = 0; j < fields.size(); j++) {
            JsonNode field = fields.get(j);

            boolean mandatory = getBoolean(field, "mandatory", false);

            String fieldId = getString(field, "id", null);
            String fieldName = getString(field, "name", null);

            if (fieldId != null) {
                recordType.addFieldTypeEntry(fieldId, mandatory);
            } else if (fieldName != null) {
                QName fieldQName = QNameConverter.fromJson(fieldName, namespaces);

                try {
                    fieldId = typeManager.getFieldTypeByName(fieldQName).getId();
                } catch (RepositoryException e) {
                    throw new JsonFormatException("Record type " + name + ": error looking up field type with name: " +
                            fieldQName, e);
                }
                recordType.addFieldTypeEntry(fieldId, mandatory);
            } else {
                throw new JsonFormatException("Record type " + name + ": field entry should specify an id or name");
            }
        }

        return recordType;
    }
}
