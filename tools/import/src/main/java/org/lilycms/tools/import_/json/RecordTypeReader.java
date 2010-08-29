package org.lilycms.tools.import_.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
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

        if (node.get("fields") != null) {
            ArrayNode fields = getArray(node, "fields");
            for (int i = 0; i < fields.size(); i++) {
                JsonNode field = fields.get(i);

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
        }

        if (node.get("mixins") != null) {
            ArrayNode mixins = getArray(node, "mixins", null);
            for (int i = 0; i < mixins.size(); i++) {
                JsonNode mixin = mixins.get(i);

                String rtId = getString(mixin, "id", null);
                String rtName = getString(mixin, "name", null);
                long rtVersion = getLong(mixin, "version", -1);

                if (rtId != null) {
                    recordType.addMixin(rtId, rtVersion == -1 ? null : rtVersion);
                } else if (rtName != null) {
                    QName rtQName = QNameConverter.fromJson(rtName, namespaces);

                    try {
                        rtId = typeManager.getRecordTypeByName(rtQName, null).getId();
                    } catch (RepositoryException e) {
                        throw new JsonFormatException("Record type " + name + ": error looking up mixin record type with name: " +
                                rtQName, e);
                    }
                    recordType.addMixin(rtId, rtVersion == -1 ? null : rtVersion);
                } else {
                    throw new JsonFormatException("Record type " + name + ": mixin should specify an id or name");
                }
            }
        }

        return recordType;
    }
}