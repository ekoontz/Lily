package org.lilycms.linkindex;

import org.lilycms.repository.api.RecordId;

/**
 * A link to some record occurring in some field.
 */
public class FieldedLink {
    private final RecordId recordId;
    private final String fieldTypeId;
    private final int hash;

    public FieldedLink(RecordId recordId, String fieldTypeId){
        this.recordId = recordId;
        this.fieldTypeId = fieldTypeId;

        int hash = 17;
        hash = 37 * hash + recordId.toString().hashCode();
        hash = 37 * hash + fieldTypeId.hashCode();
        this.hash = hash;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public String getFieldTypeId() {
        return fieldTypeId;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldedLink) {
            FieldedLink otherLink = (FieldedLink)obj;
            return recordId.equals(otherLink.recordId) && fieldTypeId.equals(otherLink.fieldTypeId);
        }
        return false;
    }
}
