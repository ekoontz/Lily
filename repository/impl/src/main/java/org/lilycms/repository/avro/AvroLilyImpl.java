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
package org.lilycms.repository.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;

public class AvroLilyImpl implements AvroLily {

    private final Repository repository;
    private final TypeManager typeManager;
    private final AvroConverter converter;

    public AvroLilyImpl(Repository repository, AvroConverter converter) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.converter = converter;
    }
    
    public AvroRecord create(AvroRecord record) throws AvroRecordExistsException, AvroRecordNotFoundException,
            AvroInvalidRecordException, AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException,
            AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.create(converter.convert(record)));
        } catch (RecordExistsException e) {
            throw converter.convert(e);
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (InvalidRecordException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public Void delete(Utf8 recordId) throws AvroRecordException {
        try {
            repository.delete(repository.getIdGenerator().fromString(recordId.toString()));
        } catch (RecordException e) {
            throw converter.convert(e);
        }
        return null;
    }

    public AvroRecord read(Utf8 recordId, long avroVersion, GenericArray<AvroQName> avroFieldNames)
            throws AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroRecordNotFoundException,
            AvroVersionNotFoundException, AvroRecordException, AvroTypeException {
        List<QName> fieldNames = null;
        if (avroFieldNames != null) {
            fieldNames = new ArrayList<QName>();
            for (AvroQName avroQName : avroFieldNames) {
                fieldNames.add(converter.convert(avroQName));
            }
        }
        try {
            Long version = null;
            if (avroVersion != -1) {
                version = avroVersion;
            }
            return converter.convert(repository.read(repository.getIdGenerator().fromString(recordId.toString()), version, fieldNames));
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (VersionNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecord update(AvroRecord record) throws AvroRecordNotFoundException, AvroInvalidRecordException,
            AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroVersionNotFoundException,
            AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.update(converter.convert(record)));
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (InvalidRecordException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (VersionNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecord updateMutableFields(AvroRecord record) throws AvroRecordNotFoundException,
            AvroInvalidRecordException, AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException,
            AvroVersionNotFoundException, AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.updateMutableFields(converter.convert(record)));
        } catch (InvalidRecordException e) {
            throw converter.convert(e);
        } catch (RecordNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (VersionNotFoundException e) {
            throw converter.convert(e);
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType createFieldType(AvroFieldType avroFieldType)
            throws AvroFieldTypeExistsException, AvroTypeException {

        try {
            return converter.convert(typeManager.createFieldType(converter.convert(avroFieldType)));
        } catch (FieldTypeExistsException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType createRecordType(AvroRecordType avroRecordType) throws AvroRecordTypeExistsException,
            AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroTypeException {

        try {
            return converter.convert(typeManager.createRecordType(converter.convert(avroRecordType)));
        } catch (RecordTypeExistsException e) {
            throw converter.convert(e);
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType getRecordTypeById(Utf8 id, long avroVersion)
            throws AvroRecordTypeNotFoundException, AvroTypeException {
        try {
            Long version = null;
            if (avroVersion != -1) {
                version = avroVersion;
            }
            return converter.convert(typeManager.getRecordTypeById(id.toString(), version));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType getRecordTypeByName(AvroQName name, long avroVersion) throws AvroRecordTypeNotFoundException,
            AvroTypeException {
        try {
            Long version = null;
            if (avroVersion != -1) {
                version = avroVersion;
            }
            return converter.convert(typeManager.getRecordTypeByName(converter.convert(name), version));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }
    
    
    public AvroRecordType updateRecordType(AvroRecordType recordType)
            throws AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroTypeException {

        try {
            return converter.convert(typeManager.updateRecordType(converter.convert(recordType)));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType updateFieldType(AvroFieldType fieldType)
            throws AvroFieldTypeNotFoundException, AvroFieldTypeUpdateException, AvroTypeException {

        try {
            return converter.convert(typeManager.updateFieldType(converter.convert(fieldType)));
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (FieldTypeUpdateException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType getFieldTypeById(Utf8 id) throws AvroFieldTypeNotFoundException, AvroTypeException {
        try {
            return converter.convert(typeManager.getFieldTypeById(id.toString()));
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType getFieldTypeByName(AvroQName name) throws AvroFieldTypeNotFoundException, AvroTypeException {
        try {
            return converter.convert(typeManager.getFieldTypeByName(converter.convert(name)));
        } catch (FieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public GenericArray<AvroFieldType> getFieldTypes() {
        return converter.convertFieldTypes(typeManager.getFieldTypes());
    }

    public GenericArray<AvroRecordType> getRecordTypes() {
        return converter.convertRecordTypes(typeManager.getRecordTypes());
    }
}
