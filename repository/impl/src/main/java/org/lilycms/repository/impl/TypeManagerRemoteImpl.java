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
import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.FieldTypeExistsException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.FieldTypeUpdateException;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.avro.*;

public class TypeManagerRemoteImpl extends AbstractTypeManager implements TypeManager {

    private AvroLily lilyProxy;
    private AvroConverter converter;

    public TypeManagerRemoteImpl(InetSocketAddress address, AvroConverter converter, IdGenerator idGenerator)
            throws IOException {
        this.converter = converter;
        //TODO idGenerator should not be available or used in the remote implementation
        this.idGenerator = idGenerator;
        HttpTransceiver client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));

        lilyProxy = (AvroLily) SpecificRequestor.getClient(AvroLily.class, client);
        initialize();
    }
    
    public RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, TypeException {

        try {
            return converter.convert(lilyProxy.createRecordType(converter.convert(recordType)));
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
        }
    }

    public RecordType getRecordType(String id, Long version) throws RecordTypeNotFoundException, TypeException {
        try {
            long avroVersion;
            if (version == null) {
                avroVersion = -1;
            } else {
                avroVersion = version;
            }
            return converter.convert(lilyProxy.getRecordType(new Utf8(id), avroVersion));
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

    public RecordType updateRecordType(RecordType recordType) throws RecordTypeNotFoundException,
            FieldTypeNotFoundException, TypeException {

        try {
            return converter.convert(lilyProxy.updateRecordType(converter.convert(recordType)));
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
        }
    }

    public FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, TypeException {
        try {
            AvroFieldType avroFieldType = converter.convert(fieldType);
            AvroFieldType createFieldType = lilyProxy.createFieldType(avroFieldType);
            FieldType resultFieldType = converter.convert(createFieldType);
            return resultFieldType;
        } catch (AvroFieldTypeExistsException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

    public FieldType updateFieldType(FieldType fieldType) throws FieldTypeNotFoundException, FieldTypeUpdateException,
            TypeException {

        try {
            return converter.convert(lilyProxy.updateFieldType(converter.convert(fieldType)));
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
        }
    }

    public FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException, TypeException {
        try {
            return converter.convert(lilyProxy.getFieldTypeById(new Utf8(id)));
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

    public FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException, TypeException {
        try {
            return converter.convert(lilyProxy.getFieldTypeByName(converter.convert(name)));
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        }
    }

}
