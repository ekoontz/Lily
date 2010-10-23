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
package org.lilyproject.linkindex;

import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.RepositoryException;

import java.util.List;
import java.util.Map;

public class RecordLinkExtractor {
    /**
     * Extracts the links from a record. The provided Record object should
     * be "fully loaded" (= contain all fields).
     */
    public static void extract(IdRecord record, LinkCollector collector, Repository repository) throws RepositoryException {
        for (Map.Entry<String, Object> field : record.getFieldsById().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = repository.getTypeManager().getFieldTypeById(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // Can not do anything with a field if we cannot load its type
                continue;
            }
            ValueType valueType = fieldType.getValueType();
            Object value = field.getValue();

            if (valueType.getPrimitive().getName().equals("LINK")) {
                extract(value, collector, fieldType.getId(), record.getId(), repository.getIdGenerator());
            } else if (valueType.getPrimitive().getName().equals("BLOB")) {
                // TODO implement link extraction from blob fields
                //      However: since blob link extraction is more expensive, we might not want to do
                //               it as a secondary action. Maybe the link index for blobs should be a
                //               completely different index.
            }
        }
    }

    private static void extract(Object value, LinkCollector collector, String fieldTypeId, RecordId ctx,
            IdGenerator idGenerator) {
        if (value instanceof List) {
            List list = (List)value;
            for (Object item : list) {
                extract(item, collector, fieldTypeId, ctx, idGenerator);
            }
        } else if (value instanceof HierarchyPath) {
            HierarchyPath path = (HierarchyPath)value;
            for (Object item : path.getElements()) {
                extract(item, collector, fieldTypeId, ctx, idGenerator);
            }
        } else if (value instanceof Link) {
            RecordId recordId = ((Link)value).resolve(ctx, idGenerator);
            collector.addLink(recordId, fieldTypeId);
        } else {
            throw new RuntimeException("Encountered an unexpected kind of object from a link field: " +
                    value.getClass().getName());
        }
    }
}
