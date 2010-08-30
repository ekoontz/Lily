package org.lilycms.tools.import_.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.FieldTypeEntry;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;

import java.util.Map;

public class RecordTypeWriter implements EntityWriter<RecordType> {
    public static EntityWriter<RecordType> INSTANCE = new RecordTypeWriter();

    public ObjectNode toJson(RecordType recordType, Repository repository) {
        Namespaces namespaces = new Namespaces();

        ObjectNode rtNode = toJson(recordType, namespaces, repository);

        rtNode.put("namespaces", NamespacesConverter.toJson(namespaces));

        return rtNode;
    }

    public ObjectNode toJson(RecordType recordType, Namespaces namespaces, Repository repository) {
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

        rtNode.put("version", recordType.getVersion());


        ArrayNode mixinsNode = rtNode.putArray("mixins");
        for (Map.Entry<String, Long> mixin : recordType.getMixins().entrySet()) {
            ObjectNode entryNode = mixinsNode.addObject();
            entryNode.put("id", mixin.getKey());
            entryNode.put("version", mixin.getValue());
        }
        
        return rtNode;
    }

}
