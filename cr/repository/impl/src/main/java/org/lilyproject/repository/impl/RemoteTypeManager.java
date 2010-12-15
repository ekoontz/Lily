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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.avro.AvroConverter;
import org.lilyproject.repository.avro.AvroFieldType;
import org.lilyproject.repository.avro.AvroFieldTypeExistsException;
import org.lilyproject.repository.avro.AvroFieldTypeNotFoundException;
import org.lilyproject.repository.avro.AvroFieldTypeUpdateException;
import org.lilyproject.repository.avro.AvroGenericException;
import org.lilyproject.repository.avro.AvroLily;
import org.lilyproject.repository.avro.AvroRecordTypeExistsException;
import org.lilyproject.repository.avro.AvroRecordTypeNotFoundException;
import org.lilyproject.repository.avro.AvroTypeException;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

// ATTENTION: when adding new methods, do not forget to add handling for UndeclaredThrowableException! This is
//            necessary because, at the time of this writing, Avro did not include IOException in its generated
//            interfaces.

public class RemoteTypeManager extends AbstractTypeManager implements TypeManager {

    private AvroLily lilyProxy;
    private AvroConverter converter;
    private HttpTransceiver client;

    public RemoteTypeManager(InetSocketAddress address, AvroConverter converter, IdGenerator idGenerator, ZooKeeperItf zooKeeper)
            throws IOException {
        super(zooKeeper);
        log = LogFactory.getLog(getClass());
        this.converter = converter;
        //TODO idGenerator should not be available or used in the remote implementation
        this.idGenerator = idGenerator;
        client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));

        lilyProxy = (AvroLily) SpecificRequestor.getClient(AvroLily.class, client);
        registerDefaultValueTypes();
    }
    
    /**
     * Start should be called for the RemoteTypeManager after the typemanager has been assigned to the repository,
     * after the repository has been assigned to the AvroConverter and before using the typemanager and repository.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void start() throws InterruptedException, KeeperException {
        setupCaches();
    }

    public void close() throws IOException {
        super.close();
        Closer.close(client);
    }

    public RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, TypeException {

        try {
            RecordType newRecordType = converter.convert(lilyProxy.createRecordType(converter.convert(recordType)));
            updateRecordTypeCache(newRecordType.clone());
            return newRecordType;
        } catch (AvroRecordTypeExistsException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    protected RecordType getRecordTypeByIdWithoutCache(String id, Long version) throws RecordTypeNotFoundException, TypeException {
        try {
            long avroVersion;
            if (version == null) {
                avroVersion = -1;
            } else {
                avroVersion = version;
            }
            return converter.convert(lilyProxy.getRecordTypeById(id, avroVersion));
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    public RecordType updateRecordType(RecordType recordType) throws RecordTypeNotFoundException,
            FieldTypeNotFoundException, TypeException {
        try {
            RecordType newRecordType = converter.convert(lilyProxy.updateRecordType(converter.convert(recordType)));
            updateRecordTypeCache(newRecordType);
            return newRecordType;
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    public FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, TypeException {
        try {
            AvroFieldType avroFieldType = converter.convert(fieldType);
            AvroFieldType createFieldType = lilyProxy.createFieldType(avroFieldType);
            FieldType newFieldType = converter.convert(createFieldType);
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroFieldTypeExistsException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    public FieldType updateFieldType(FieldType fieldType) throws FieldTypeNotFoundException, FieldTypeUpdateException,
            TypeException {

        try {
            FieldType newFieldType = converter.convert(lilyProxy.updateFieldType(converter.convert(fieldType)));
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeUpdateException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    public List<FieldType> getFieldTypesWithoutCache() throws TypeException {
        try {
            return converter.convertAvroFieldTypes(lilyProxy.getFieldTypes());
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    public List<RecordType> getRecordTypesWithoutCache() throws TypeException {
        try {
            return converter.convertAvroRecordTypes(lilyProxy.getRecordTypes());
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    private RuntimeException handleUndeclaredTypeThrowable(UndeclaredThrowableException e) throws TypeException {
        if (e.getCause() instanceof IOException) {
            throw new IOTypeException(e.getCause());
        } else {
            throw e;
        }
    }
}
