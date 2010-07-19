package org.lilycms.repository.api;

import org.lilycms.repository.api.FieldNotFoundException;

import java.util.Map;

/**
 * <b>EXPERT:</b> A record in which the fields are also identified by ID instead of only by QName.
 *
 * <p>In a normal Record, fields are only loaded with their QName. On the storage level, fields are identified
 * by ID. After a Record is read, the QNames of field types in the repository can change, making it impossible
 * to reliably map the QName again to the field type (the old QName might now point to another field type). For most
 * applications this will pose no problems. But if you need to know the exact identity of the fields, you can use
 * this class.
 *
 * <p>IdRecord is meant for read-only purposes. If you start modifying the record, the operation of the ID-based
 * methods defined in this interface is not guaranteed and might produce incorrect results.
 */
public interface IdRecord extends Record {
    Object getField(String fieldId) throws FieldNotFoundException;

    boolean hasField(String fieldId);

    Map<String, Object> getFieldsById();

    /**
     * Returns the underlying "normal" record.
     */
    Record getRecord();
}
