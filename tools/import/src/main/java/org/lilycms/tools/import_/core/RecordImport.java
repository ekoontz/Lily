package org.lilycms.tools.import_.core;

import org.lilycms.repository.api.*;
import org.lilycms.util.ObjectUtils;

import java.util.Map;

import static org.lilycms.tools.import_.core.ImportMode.*;

public class RecordImport {
    public static ImportResult<Record> importRecord(Record newRecord, ImportMode impMode, Repository repository)
            throws RepositoryException {

        int loopCount = 0;
        while (true) {
            if (loopCount > 1) {
                // We should never arrive here
                throw new RuntimeException("Unexpected situation: when we tried to update the record, " +
                        "it did not exist, when we tried to create the record, it exists, and then when we retry " +
                        "to update, it does not exist after all.");
            }

            if (impMode == UPDATE || impMode == CREATE_OR_UPDATE) {
                Record oldRecord = null;
                if (newRecord.getId() != null) {
                    try {
                        oldRecord = repository.read(newRecord.getId());
                    } catch (RecordNotFoundException e) {
                        if (impMode == UPDATE) {
                            return ImportResult.cannotUpdateDoesNotExist();
                        }
                    }
                } else if (impMode == UPDATE) {
                    return ImportResult.cannotUpdateDoesNotExist();                    
                }

                if (oldRecord != null) {
                    boolean updated = false;

                    // Handle the fields
                    for (QName name : newRecord.getFieldsToDelete()) {
                        if (oldRecord.hasField(name)) {
                            updated = true;
                            break;
                        }
                    }

                    for (Map.Entry<QName, Object> entry : newRecord.getFields().entrySet()) {
                        if (!oldRecord.hasField(entry.getKey())) {
                            updated = true;
                            break;
                        } else if (!ObjectUtils.safeEquals(oldRecord.getField(entry.getKey()), entry.getValue())) {
                            updated = true;
                            break;
                        }
                    }

                    // Handle record type
                    if (newRecord.getRecordTypeName() != null) {
                        if (!newRecord.getRecordTypeName().equals(oldRecord.getRecordTypeName())) {
                            updated = true;
                        } else if (newRecord.getRecordTypeVersion() != null && !newRecord.getRecordTypeVersion().equals(oldRecord.getRecordTypeVersion())) {
                            updated = true;
                        } else if (newRecord.getRecordTypeVersion() == null) {
                            // when the version type is null, this means a request to update the record to the last version
                            // of the record type, so check if the old record already is at this version
                            long lastVersion = repository.getTypeManager().getRecordTypeByName(newRecord.getRecordTypeName(), null).getVersion();
                            if (oldRecord.getRecordTypeVersion() != lastVersion) {
                                updated = true;
                            }
                        }
                    }

                    if (updated) {
                        // TODO repository.update() should be able to return a record will all fields loaded
                        Record updatedRecord = repository.update(newRecord);
                        return ImportResult.updated(updatedRecord);
                    } else {
                        // TODO: we moeten hier eigenlijk het record retourneren met de gewenste velden erin
                        //  wellicht best door post-filtering, en niet aan de repository te vragen (want zou
                        //  weer een inconsistente toestand kunnen geven)
                        // We return the old record since that one has everything 'filled in'
                        return ImportResult.upToDate(oldRecord);
                    }

                }
            }

            if (impMode == UPDATE) {
                // We should never arrive here, update is handled above
                throw new RuntimeException("Unexpected situation: in case of mode " + UPDATE + " we should not be here.");
            }

            try {
                Record createdRecord = repository.create(newRecord);
                return ImportResult.created(createdRecord);
            } catch (RecordExistsException e) {
                if (impMode == CREATE) {
                    return ImportResult.cannotCreateExists();
                }
                // and otherwise, the record has been created since we last checked, so we now
                // loop again to the top to try to update it
            }

            loopCount++;
        }

    }
}
