/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;

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
