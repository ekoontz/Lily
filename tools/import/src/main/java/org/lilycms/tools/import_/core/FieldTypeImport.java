package org.lilycms.tools.import_.core;

import org.lilycms.repository.api.*;

public class FieldTypeImport {

    public static ImportResult<FieldType> importFieldType(FieldType newFieldType, ImportMode impMode,
            IdentificationMode idMode, QName identifyingName, TypeManager typeManager) throws RepositoryException {

        if (idMode == IdentificationMode.ID && impMode == ImportMode.CREATE_OR_UPDATE) {
            throw new IllegalArgumentException("The combination of import mode " + ImportMode.CREATE_OR_UPDATE
                    + " and identification mode " + IdentificationMode.ID + " is not possible.");
        }

        int loopCount = 0;
        while (true) {
            if (loopCount > 1) {
                // We should never arrive here
                throw new RuntimeException("Unexpected situation: when we tried to update the field type, " +
                        "it did not exist, when we tried to create the field type, it exists, and then when we retry " +
                        "to update, it does not exist after all.");
            }

            if (impMode == ImportMode.UPDATE || impMode == ImportMode.CREATE_OR_UPDATE) {
                FieldType oldFieldType = null;
                try {
                    if (idMode == IdentificationMode.ID) {
                        oldFieldType = typeManager.getFieldTypeById(newFieldType.getId());                        
                    } else {
                        oldFieldType = typeManager.getFieldTypeByName(identifyingName);
                    }
                } catch (FieldTypeNotFoundException e) {
                    if (impMode == ImportMode.UPDATE) {
                        return ImportResult.cannotUpdateDoesNotExist();
                    }
                }

                if (oldFieldType != null) {
                    boolean updated = false;

                    // Check non-mutable fields are equal
                    String oldPrimitive = oldFieldType.getValueType().getPrimitive().getName();
                    String newPrimitive = newFieldType.getValueType().getPrimitive().getName();
                    if (!oldPrimitive.equals(newPrimitive)) {
                        return ImportResult.conflict("primitive type", oldPrimitive, newPrimitive);
                    }

                    boolean oldMultivalue = oldFieldType.getValueType().isMultiValue();
                    boolean newMultiValue = newFieldType.getValueType().isMultiValue();
                    if (oldMultivalue != newMultiValue) {
                        return ImportResult.conflict("multi-value", oldMultivalue, newMultiValue);
                    }

                    boolean oldHierarchical = oldFieldType.getValueType().isHierarchical();
                    boolean newHierarchical = newFieldType.getValueType().isHierarchical();
                    if (oldHierarchical != newHierarchical) {
                        return ImportResult.conflict("hierarchical", oldMultivalue, newMultiValue);
                    }

                    Scope oldScope = oldFieldType.getScope();
                    Scope newScope = newFieldType.getScope();
                    if (!oldScope.equals(newScope)) {
                        return ImportResult.conflict("scope", oldScope, newScope);
                    }

                    // Update mutable fields
                    QName oldName = oldFieldType.getName();
                    QName newName = newFieldType.getName();
                    if (!oldName.equals(newName)) {
                        updated = true;
                        oldFieldType.setName(newName);
                    }

                    if (updated) {
                        oldFieldType = typeManager.updateFieldType(oldFieldType);
                        return ImportResult.updated(oldFieldType);
                    } else {
                        return ImportResult.upToDate(oldFieldType);
                    }
                }
            }

            if (impMode == ImportMode.UPDATE) {
                // We should never arrive here, update is handled above
                throw new RuntimeException("Unexpected situation: in case of mode " + ImportMode.UPDATE + " we should not be here.");
            }

            try {
                FieldType createdFieldType = typeManager.createFieldType(newFieldType);
                return ImportResult.created(createdFieldType);
            } catch (FieldTypeExistsException e) {
                if (impMode == ImportMode.CREATE) {
                    return ImportResult.cannotCreateExists();
                }
                // and otherwise, the field type has been created since we last checked, so we now
                // loop again to the top to try to update it
            }

            loopCount++;
        }
    }

}
