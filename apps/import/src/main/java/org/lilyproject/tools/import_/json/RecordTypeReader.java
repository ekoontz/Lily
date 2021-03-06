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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.*;

import static org.lilyproject.util.json.JsonUtil.*;

public class RecordTypeReader implements EntityReader<RecordType> {
    public static EntityReader<RecordType> INSTANCE  = new RecordTypeReader();

    public RecordType fromJson(ObjectNode node, Repository repository) throws JsonFormatException, RepositoryException,
            InterruptedException {
        Namespaces namespaces = NamespacesConverter.fromContextJson(node);
        return fromJson(node, namespaces, repository);
    }

    public RecordType fromJson(ObjectNode node, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {

        TypeManager typeManager = repository.getTypeManager();
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
                Long rtVersion = getLong(mixin, "version", null);

                if (rtId != null) {
                    recordType.addMixin(rtId, rtVersion);
                } else if (rtName != null) {
                    QName rtQName = QNameConverter.fromJson(rtName, namespaces);

                    try {
                        rtId = typeManager.getRecordTypeByName(rtQName, null).getId();
                    } catch (RepositoryException e) {
                        throw new JsonFormatException("Record type " + name + ": error looking up mixin record type with name: " +
                                rtQName, e);
                    }
                    recordType.addMixin(rtId, rtVersion);
                } else {
                    throw new JsonFormatException("Record type " + name + ": mixin should specify an id or name");
                }
            }
        }

        return recordType;
    }
}
