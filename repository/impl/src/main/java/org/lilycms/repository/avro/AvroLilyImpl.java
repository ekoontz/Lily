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
import org.lilycms.repository.api.BlobException;
import org.lilycms.repository.api.BlobNotFoundException;
import org.lilycms.repository.api.FieldTypeExistsException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.FieldTypeUpdateException;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordException;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.VersionNotFoundException;

public class AvroLilyImpl implements AvroLily {

    private final Repository repository;
    private final TypeManager typeManager;
    private final AvroConverter converter;

    public AvroLilyImpl(Repository repository, AvroConverter converter) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.converter = converter;
    }

    public AvroRecord create(AvroRecord record) throws AvroRecordExistsException,
            AvroInvalidRecordException, AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException,
            AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.create(converter.convert(record)));
        } catch (RecordExistsException e) {
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

    public Void delete(Utf8 recordId) throws AvroRecordException, AvroTypeException, AvroFieldTypeNotFoundException,
            AvroRecordNotFoundException {
        try {
            repository.delete(converter.convertAvroRecordId(recordId));
        } catch (RecordException e) {
            throw converter.convert(e);
        } catch (RecordNotFoundException e) {
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
            return converter.convert(repository.read(converter.convertAvroRecordId(recordId),
                    converter.convertAvroVersion(avroVersion), fieldNames));
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

    public GenericArray<AvroRecord> readVersions(Utf8 recordId, long avroFromVersion, long avroToVersion,
            GenericArray<AvroQName> avroFieldNames) throws AvroRecordTypeNotFoundException,
            AvroFieldTypeNotFoundException, AvroRecordNotFoundException, AvroVersionNotFoundException,
            AvroRecordException, AvroTypeException {
        List<QName> fieldNames = null;
        if (avroFieldNames != null) {
            fieldNames = new ArrayList<QName>();
            for (AvroQName avroQName : avroFieldNames) {
                fieldNames.add(converter.convert(avroQName));
            }
        }
        try {
            return converter.convertRecords(repository.readVersions(converter.convertAvroRecordId(
                    recordId), converter.convertAvroVersion(avroFromVersion), converter.convertAvroVersion(avroToVersion), fieldNames));
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

    public AvroRecord update(AvroRecord record, boolean updateVersion, boolean useLatestRecordType) throws AvroRecordNotFoundException,
            AvroInvalidRecordException, AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException,
            AvroVersionNotFoundException, AvroRecordException, AvroTypeException {
        try {
            return converter.convert(repository.update(converter.convert(record), updateVersion, useLatestRecordType));
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

    public AvroFieldType createFieldType(AvroFieldType avroFieldType) throws AvroFieldTypeExistsException,
            AvroTypeException {

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

    public AvroRecordType getRecordTypeById(Utf8 id, long avroVersion) throws AvroRecordTypeNotFoundException,
            AvroTypeException {
        try {
            return converter.convert(typeManager.getRecordTypeById(id.toString(), converter.convertAvroVersion(avroVersion)));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType getRecordTypeByName(AvroQName name, long avroVersion) throws AvroRecordTypeNotFoundException,
            AvroTypeException {
        try {
            return converter.convert(typeManager.getRecordTypeByName(converter.convert(name), converter.convertAvroVersion(avroVersion)));
        } catch (RecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType updateRecordType(AvroRecordType recordType) throws AvroRecordTypeNotFoundException,
            AvroFieldTypeNotFoundException, AvroTypeException {

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

    public AvroFieldType updateFieldType(AvroFieldType fieldType) throws AvroFieldTypeNotFoundException,
            AvroFieldTypeUpdateException, AvroTypeException {

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

    public GenericArray<AvroFieldType> getFieldTypes() throws AvroTypeException {
        try {
            return converter.convertFieldTypes(typeManager.getFieldTypes());
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public GenericArray<AvroRecordType> getRecordTypes() throws AvroTypeException {
        try {
            return converter.convertRecordTypes(typeManager.getRecordTypes());
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public GenericArray<Utf8> getVariants(Utf8 recordId) throws AvroRemoteException, AvroRepositoryException,
            AvroGenericException {
        try {
            return converter.convert(repository.getVariants(converter.convertAvroRecordId(recordId)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        }
    }

    public AvroIdRecord readWithIds(Utf8 recordId, long avroVersion, GenericArray<Utf8> avroFieldIds)
            throws AvroRemoteException, AvroRecordNotFoundException, AvroVersionNotFoundException,
            AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroRecordException, AvroTypeException,
            AvroGenericException {
        try {
            List<String> fieldIds = null;
            if (avroFieldIds != null) {
                fieldIds = new ArrayList<String>();
                for (Utf8 utf8 : avroFieldIds) {
                    fieldIds.add(converter.convert(utf8));
                }
            }
            return converter.convert(repository.readWithIds(converter.convertAvroRecordId(recordId), converter.convertAvroVersion(avroVersion), fieldIds));
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

    public Void deleteBlob(AvroBlob avroBlob) throws AvroRemoteException, AvroBlobNotFoundException, AvroBlobException {
        try {
            repository.delete(converter.convert(avroBlob));
        } catch (BlobNotFoundException e) {
            throw converter.convert(e);
        } catch (BlobException e) {
            throw converter.convert(e);
        }
        return null;
    }

}
