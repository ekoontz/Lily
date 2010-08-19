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
package org.lilycms.repository.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobException;
import org.lilycms.repository.api.BlobNotFoundException;
import org.lilycms.repository.api.BlobStoreAccess;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.IdRecord;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordException;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.VersionNotFoundException;
import org.lilycms.repository.avro.AvroBlobException;
import org.lilycms.repository.avro.AvroBlobNotFoundException;
import org.lilycms.repository.avro.AvroConverter;
import org.lilycms.repository.avro.AvroFieldTypeNotFoundException;
import org.lilycms.repository.avro.AvroGenericException;
import org.lilycms.repository.avro.AvroInvalidRecordException;
import org.lilycms.repository.avro.AvroLily;
import org.lilycms.repository.avro.AvroQName;
import org.lilycms.repository.avro.AvroRecordException;
import org.lilycms.repository.avro.AvroRecordExistsException;
import org.lilycms.repository.avro.AvroRecordNotFoundException;
import org.lilycms.repository.avro.AvroRecordTypeNotFoundException;
import org.lilycms.repository.avro.AvroRepositoryException;
import org.lilycms.repository.avro.AvroTypeException;
import org.lilycms.repository.avro.AvroVersionNotFoundException;
import org.lilycms.util.ArgumentValidator;

public class RemoteRepository implements Repository {
    private AvroLily lilyProxy;
    private final AvroConverter converter;
    private IdGenerator idGenerator;
    private final TypeManager typeManager;
    private BlobStoreAccessRegistry blobStoreAccessRegistry;

    public RemoteRepository(InetSocketAddress address, AvroConverter converter, RemoteTypeManager typeManager, IdGenerator idGenerator, BlobStoreAccessFactory blobStoreAccessFactory)
            throws IOException {
        this.converter = converter;
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        blobStoreAccessRegistry = new BlobStoreAccessRegistry();
        blobStoreAccessRegistry.setBlobStoreAccessFactory(blobStoreAccessFactory);

        HttpTransceiver client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));

        lilyProxy = (AvroLily) SpecificRequestor.getClient(AvroLily.class, client);
    }
    
    public TypeManager getTypeManager() {
        return typeManager;
    }
    
    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }
    
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }
    
    public Record create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException {
        try {
            return converter.convert(lilyProxy.create(converter.convert(record)));
        } catch (AvroRecordExistsException e) {
            throw converter.convert(e);
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroInvalidRecordException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

    public void delete(RecordId recordId) throws RecordException {
        try {
            lilyProxy.delete(converter.convert(recordId));
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, VersionNotFoundException, RecordException, TypeException {
        return read(recordId, null, null);
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, VersionNotFoundException, RecordException,
            TypeException {
        return read(recordId, null, fieldNames);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, VersionNotFoundException, RecordException, TypeException {
        return read(recordId, version, null);
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, VersionNotFoundException, RecordException,
            TypeException {
        try {
            GenericArray<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new GenericData.Array<AvroQName>(fieldNames.size(), Schema.createArray(AvroQName.SCHEMA$));
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convert(lilyProxy.read(converter.convert(recordId), converter.convertVersion(version), avroFieldNames));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }
    
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException {
        try {
            GenericArray<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new GenericData.Array<AvroQName>(fieldNames.size(), Schema.createArray(AvroQName.SCHEMA$));
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convertAvroRecords(lilyProxy.readVersions(converter.convert(recordId), converter.convertVersion(fromVersion), converter.convertVersion(toVersion), avroFieldNames));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }
    
    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException, VersionNotFoundException {
        return update(record, false, true);
    }

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType) throws RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException, VersionNotFoundException {
        try {
            return converter.convert(lilyProxy.update(converter.convert(record), updateVersion, useLatestRecordType));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroInvalidRecordException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }
    
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException {
        try {
            return converter.convertAvroRecordIds(lilyProxy.getVariants(converter.convert(recordId)));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }
    
    public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) throws RecordNotFoundException, VersionNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException {
        try {
            GenericArray<Utf8> avroFieldIds = null;
            if (fieldIds != null) {
                avroFieldIds = new GenericData.Array<Utf8>(fieldIds.size(), Schema.createArray(Schema.create(Schema.Type.STRING)));
                for (String fieldId : fieldIds) {
                    avroFieldIds.add(converter.convert(fieldId));
                }
            }
            return converter.convert(lilyProxy.readWithIds(converter.convert(recordId), converter.convertVersion(version), avroFieldIds));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobStoreAccessRegistry.register(blobStoreAccess);
    }
    
    public void delete(Blob blob) throws BlobNotFoundException, BlobException {
        try {
            lilyProxy.deleteBlob(converter.convert(blob));
        } catch (AvroBlobNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroBlobException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }
    
    public InputStream getInputStream(Blob blob) throws BlobNotFoundException, BlobException {
        return blobStoreAccessRegistry.getInputStream(blob);
    }
    
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return blobStoreAccessRegistry.getOutputStream(blob);
    }
}

