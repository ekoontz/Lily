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
package org.lilycms.tools.import_;

import org.lilycms.repository.api.*;
import org.lilycms.util.repo.VersionTag;

import java.util.*;

public class ImportTool {
    private Repository repository;
    private TypeManager typeManager;
    private ImportListener importListener;

    public ImportTool(Repository repository, ImportListener importListener) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.importListener = importListener;
    }

    public FieldType importFieldType(FieldType newFieldType) throws RepositoryException, ImportConflictException {
        FieldType oldFieldType = null;
        try {
            oldFieldType = typeManager.getFieldTypeByName(newFieldType.getName());
        } catch (FieldTypeNotFoundException e) {
            // ok
        }

        if (oldFieldType != null) {
            // check it is similar
            String oldPrimitive = oldFieldType.getValueType().getPrimitive().getName();
            String newPrimitive = newFieldType.getValueType().getPrimitive().getName();
            checkEquals(oldPrimitive, newPrimitive, EntityType.FIELD_TYPE, "primitive type", newFieldType.getName().toString());

            boolean oldMultivalue = oldFieldType.getValueType().isMultiValue();
            boolean newMultiValue = newFieldType.getValueType().isMultiValue();
            checkEquals(oldMultivalue, newMultiValue, EntityType.FIELD_TYPE, "multi-value", newFieldType.getName().toString());

            boolean oldHierarchical = oldFieldType.getValueType().isHierarchical();
            boolean newHierarchical = newFieldType.getValueType().isHierarchical();
            checkEquals(oldHierarchical, newHierarchical, EntityType.FIELD_TYPE, "hierarchical", newFieldType.getName().toString());

            Scope oldScope = oldFieldType.getScope();
            Scope newScope = newFieldType.getScope();
            checkEquals(oldScope, newScope, EntityType.FIELD_TYPE, "scope", newFieldType.getName().toString());

            // everything equal, skip it
            importListener.existsAndEqual(EntityType.FIELD_TYPE, newFieldType.getName().toString(), null);
            return oldFieldType;
        }

        FieldType createdFieldType = typeManager.createFieldType(newFieldType);
        importListener.created(EntityType.FIELD_TYPE, createdFieldType.getName().toString(), createdFieldType.getId());
        return createdFieldType;
    }

    public RecordType importRecordType(RecordType newRecordType) throws RepositoryException {
        RecordType oldRecordType = null;
        try {
            oldRecordType = typeManager.getRecordType(newRecordType.getId(), null);
        } catch (RecordTypeNotFoundException e) {
            // ok
        }

        if (oldRecordType != null) {
            // check it is similar
            Set<FieldTypeEntry> oldFieldTypeEntries = new HashSet<FieldTypeEntry>(oldRecordType.getFieldTypeEntries());
            Set<FieldTypeEntry> newFieldTypeEntries = new HashSet<FieldTypeEntry>(newRecordType.getFieldTypeEntries());
            boolean updated = false;
            if (!newFieldTypeEntries.equals(oldFieldTypeEntries)) {
                updated = true;
                // update the record type
                for (FieldTypeEntry entry : newFieldTypeEntries) {
                    if (oldRecordType.getFieldTypeEntry(entry.getFieldTypeId()) == null) {
                        oldRecordType.addFieldTypeEntry(entry);
                    }
                }
                for (FieldTypeEntry entry : oldFieldTypeEntries) {
                    if (newRecordType.getFieldTypeEntry(entry.getFieldTypeId()) == null) {
                        oldRecordType.removeFieldTypeEntry(entry.getFieldTypeId());
                    }
                }
            }

            // TODO mixins


            if (updated) {
                oldRecordType = typeManager.updateRecordType(oldRecordType);
                importListener.updated(EntityType.RECORD_TYPE, null, oldRecordType.getId(), oldRecordType.getVersion());
            } else {
                importListener.existsAndEqual(EntityType.RECORD_TYPE, null, oldRecordType.getId());
            }
            return oldRecordType;
        } else {
            RecordType createdRecordType = typeManager.createRecordType(newRecordType);
            importListener.created(EntityType.RECORD_TYPE, null, createdRecordType.getId());
            return createdRecordType;
        }
    }

    public Record importRecord(Record newRecord) throws RepositoryException {
        Record oldRecord = null;
        if (newRecord.getId() != null) {
            try {
                oldRecord = repository.read(newRecord.getId());
            } catch (RecordNotFoundException e) {
                // ok
            }
        }

        if (oldRecord != null) {
            // Collect the set of vtag fields that should be automatically set to the last version.
            // These are by contract those that have the value -1.
            Set<QName> vtagFieldsToBeSetToLastVersion = new HashSet<QName>();
            for (Map.Entry<QName, Object> field : newRecord.getFields().entrySet()) {
                if (field.getKey().getNamespace().equals(VersionTag.NAMESPACE) && field.getValue().equals(-1L)) {
                    vtagFieldsToBeSetToLastVersion.add(field.getKey());
                }
            }

            // Before comparing with the previous state, set the vtag fields to the currently last version
            for (QName vtag : vtagFieldsToBeSetToLastVersion) {
                newRecord.setField(vtag, oldRecord.getVersion());
            }

            if (newRecord.softEquals(oldRecord)) {
                importListener.existsAndEqual(EntityType.RECORD, null, newRecord.getId().toString());
                return oldRecord;
            } else {
                // Delete fields which are not present in the new record anymore
                for (Map.Entry<QName, Object> field : oldRecord.getFields().entrySet()) {
                    if (!newRecord.hasField(field.getKey())) {
                        newRecord.delete(field.getKey(), true);
                    }
                }

                // Exclude vtag fields to-be-set-to-last-version from the update, will set those afterwards
                for (QName vtag : vtagFieldsToBeSetToLastVersion) {
                    newRecord.delete(vtag, false);
                }

                // Update the record
                Record updatedRecord = repository.update(newRecord);

                // Now do a second update to set the vtags to version created
                if (updatedRecord.getVersion() != null && !vtagFieldsToBeSetToLastVersion.isEmpty()) {
                    Record vtagsUpdate = repository.newRecord(newRecord.getId());
                    for (QName vtag : vtagFieldsToBeSetToLastVersion) {
                        if (!(oldRecord.hasField(vtag) && oldRecord.getField(vtag).equals(updatedRecord.getVersion()))) {
                            vtagsUpdate.setField(vtag, updatedRecord.getVersion());
                        }
                    }

                    if (!vtagsUpdate.getFields().isEmpty()) {
                        vtagsUpdate.setRecordType(updatedRecord.getRecordTypeId(), updatedRecord.getRecordTypeVersion());
                        updatedRecord = repository.update(vtagsUpdate);
                    }
                }

                importListener.updated(EntityType.RECORD, null, updatedRecord.getId().toString(), updatedRecord.getVersion());
                return updatedRecord;
            }
        } else {
            // For new records, let the vtag fields which should point to the last version point to 1
            // TODO: this should check if there are actually any versioned fields.
            for (Map.Entry<QName, Object> field : newRecord.getFields().entrySet()) {
                if (field.getKey().getNamespace().equals(VersionTag.NAMESPACE) && field.getValue().equals(-1L)) {
                    newRecord.setField(field.getKey(), 1L);
                }
            }
            Record createdRecord = repository.create(newRecord);
            importListener.created(EntityType.RECORD, null, createdRecord.getId().toString());
            return createdRecord;
        }
    }

    private void checkEquals(Object oldValue, Object newValue, EntityType entityType, String propName, String entityName)
            throws ImportConflictException {
        if (!oldValue.equals(newValue)) {
            importListener.conflict(entityType, entityName, propName, oldValue, newValue);
        }
    }
}
