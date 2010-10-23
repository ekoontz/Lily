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
package org.lilyproject.repository.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.ipc.AvroRemoteException;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.FieldTypeExistsException;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.FieldTypeUpdateException;
import org.lilyproject.repository.api.InvalidRecordException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordExistsException;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordTypeExistsException;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.VersionNotFoundException;

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

    public Void delete(CharSequence recordId) throws AvroRecordException, AvroTypeException, AvroFieldTypeNotFoundException,
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

    public AvroRecord read(CharSequence recordId, long avroVersion, List<AvroQName> avroFieldNames)
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

    public List<AvroRecord> readVersions(CharSequence recordId, long avroFromVersion, long avroToVersion,
            List<AvroQName> avroFieldNames) throws AvroRecordTypeNotFoundException,
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

    public AvroRecordType getRecordTypeById(CharSequence id, long avroVersion) throws AvroRecordTypeNotFoundException,
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

    public AvroFieldType getFieldTypeById(CharSequence id) throws AvroFieldTypeNotFoundException, AvroTypeException {
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

    public List<AvroFieldType> getFieldTypes() throws AvroTypeException {
        try {
            return converter.convertFieldTypes(typeManager.getFieldTypes());
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public List<AvroRecordType> getRecordTypes() throws AvroTypeException {
        try {
            return converter.convertRecordTypes(typeManager.getRecordTypes());
        } catch (TypeException e) {
            throw converter.convert(e);
        }
    }

    public List<CharSequence> getVariants(CharSequence recordId) throws AvroRemoteException, AvroRepositoryException,
            AvroGenericException {
        try {
            return converter.convert(repository.getVariants(converter.convertAvroRecordId(recordId)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        }
    }

    public AvroIdRecord readWithIds(CharSequence recordId, long avroVersion, List<CharSequence> avroFieldIds)
            throws AvroRemoteException, AvroRecordNotFoundException, AvroVersionNotFoundException,
            AvroRecordTypeNotFoundException, AvroFieldTypeNotFoundException, AvroRecordException, AvroTypeException,
            AvroGenericException {
        try {
            List<String> fieldIds = null;
            if (avroFieldIds != null) {
                fieldIds = new ArrayList<String>();
                for (CharSequence avroFieldId : avroFieldIds) {
                    fieldIds.add(converter.convert(avroFieldId));
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
