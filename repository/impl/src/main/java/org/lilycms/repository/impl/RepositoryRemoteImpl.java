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
import org.apache.commons.lang.NotImplementedException;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.BlobNotFoundException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.avro.*;
import org.lilycms.util.ArgumentValidator;

public class RepositoryRemoteImpl implements Repository {
    private AvroLily lilyProxy;
    private final AvroConverter converter;
    private IdGenerator idGenerator;
    private final TypeManager typeManager;

    public RepositoryRemoteImpl(InetSocketAddress address, AvroConverter converter, TypeManagerRemoteImpl typeManager, IdGenerator idGenerator)
            throws IOException {
        this.converter = converter;
        this.typeManager = typeManager;
        //TODO idGenerator should not be available or used in the remote implementation
        this.idGenerator = idGenerator;
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
            lilyProxy.delete(new Utf8(recordId.toString()));
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
            long avroVersion;
            if (version == null) {
                avroVersion = -1L;
            } else {
                avroVersion = version;
            }
            GenericArray<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new GenericData.Array<AvroQName>(fieldNames.size(), Schema.createArray(AvroQName.SCHEMA$));
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convert(lilyProxy.read(new Utf8(recordId.toString()), avroVersion, avroFieldNames));
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

    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException, VersionNotFoundException {
        try {
            return converter.convert(lilyProxy.update(converter.convert(record)));
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
    
    public Record updateMutableFields(Record record) throws InvalidRecordException, RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, TypeException, RecordException, VersionNotFoundException {
        try {
            return converter.convert(lilyProxy.updateMutableFields(converter.convert(record)));
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

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        throw new NotImplementedException();
    }
    
    
    public void delete(Blob blob) throws BlobNotFoundException, BlobException {
        throw new NotImplementedException();
    }
    
    public InputStream getInputStream(Blob blob) throws BlobNotFoundException, BlobException {
        throw new NotImplementedException();
    }
    
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        throw new NotImplementedException();
    }

    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException {
        throw new NotImplementedException();
    }

    public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) {
        throw new NotImplementedException();
    }
}

