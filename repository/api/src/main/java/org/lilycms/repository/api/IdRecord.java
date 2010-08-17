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
package org.lilycms.repository.api;

import org.lilycms.repository.api.FieldNotFoundException;

import java.util.Map;

/**
 * <b>EXPERT:</b> A record in which the fields and record types are also identified by ID instead of only by QName.
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
    String getRecordTypeId();

    String getRecordTypeId(Scope scope);

    Object getField(String fieldId) throws FieldNotFoundException;

    boolean hasField(String fieldId);

    Map<String, Object> getFieldsById();

    /**
     * Returns the underlying "normal" record.
     */
    Record getRecord();
    
    /**
     * To be used by AvroConverter only
     */
    Map<String, QName> getFieldIdToNameMapping();
}
