package org.lilycms.linkindex;

import org.lilycms.repository.api.*;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.RepositoryException;

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
