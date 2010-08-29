package org.lilycms.rest.import_;

import org.lilycms.repository.api.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.lilycms.rest.import_.ImportMode.*;

public class RecordTypeImport {
    public static ImportResult<RecordType> importRecordType(RecordType newRecordType, ImportMode impMode,
            IdentificationMode idMode, QName identifyingName, TypeManager typeManager) throws RepositoryException {

        if (idMode == IdentificationMode.ID && impMode == CREATE_OR_UPDATE) {
            throw new IllegalArgumentException("The combination of import mode " + CREATE_OR_UPDATE
                    + " and identification mode " + IdentificationMode.ID + " is not possible.");
        }

        int loopCount = 0;
        while (true) {
            if (loopCount > 1) {
                // We should never arrive here
                throw new RuntimeException("Unexpected situation: when we tried to update the record type, " +
                        "it did not exist, when we tried to create the record type, it exists, and then when we retry " +
                        "to update, it does not exist after all.");
            }

            if (impMode == UPDATE || impMode == CREATE_OR_UPDATE) {
                RecordType oldRecordType = null;
                try {
                    if (idMode == IdentificationMode.ID) {
                        oldRecordType = typeManager.getRecordTypeById(newRecordType.getId(), null);
                    } else {
                        oldRecordType = typeManager.getRecordTypeByName(identifyingName, null);
                    }
                } catch (RecordTypeNotFoundException e) {
                    if (impMode == UPDATE) {
                        return ImportResult.cannotUpdateDoesNotExist();
                    }
                }

                if (oldRecordType != null) {
                    boolean updated = false;

                    // Update field entries
                    Set<FieldTypeEntry> oldFieldTypeEntries = new HashSet<FieldTypeEntry>(oldRecordType.getFieldTypeEntries());
                    Set<FieldTypeEntry> newFieldTypeEntries = new HashSet<FieldTypeEntry>(newRecordType.getFieldTypeEntries());
                    if (!newFieldTypeEntries.equals(oldFieldTypeEntries)) {
                        updated = true;

                        oldRecordType.getFieldTypeEntries().clear();

                        for (FieldTypeEntry entry : newFieldTypeEntries) {
                            oldRecordType.addFieldTypeEntry(entry);
                        }
                    }

                    // Update mixins
                    Map<String, Long> oldMixins = oldRecordType.getMixins();
                    Map<String, Long> newMixins = newRecordType.getMixins();

                    // Resolve any 'null' versions to actual version numbers, otherwise we are unable to compare
                    // with the old state.
                    for (Map.Entry<String, Long> entry : newMixins.entrySet()) {
                        if (entry.getValue() == null) {
                            entry.setValue(typeManager.getRecordTypeById(entry.getKey(), null).getVersion());
                        }
                    }

                    if (!oldMixins.equals(newMixins)) {
                        updated = true;

                        oldRecordType.getMixins().clear();

                        for (Map.Entry<String, Long> entry : newMixins.entrySet()) {
                            oldRecordType.addMixin(entry.getKey(), entry.getValue());
                        }
                    }

                    // Update name
                    QName oldName = oldRecordType.getName();
                    QName newName = newRecordType.getName();
                    if (!oldName.equals(newName)) {
                        updated = true;
                        oldRecordType.setName(newName);
                    }

                    if (updated) {
                        oldRecordType = typeManager.updateRecordType(oldRecordType);
                        return ImportResult.updated(oldRecordType);
                    } else {
                        return ImportResult.upToDate(oldRecordType);
                    }
                }
            }

            if (impMode == UPDATE) {
                // We should never arrive here, update is handled above
                throw new RuntimeException("Unexpected situation: in case of mode " + UPDATE + " we should not be here.");
            }

            try {
                RecordType createdRecordType = typeManager.createRecordType(newRecordType);
                return ImportResult.created(createdRecordType);
            } catch (RecordTypeExistsException e) {
                if (impMode == CREATE) {
                    return ImportResult.cannotCreateExists();
                }
                // and otherwise, the record type has been created since we last checked, so we now
                // loop again to the top to try to update it
            }

            loopCount++;
        }

    }
}
