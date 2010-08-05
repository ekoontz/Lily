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
package org.lilycms.repository.impl.test;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.AbstractTypeManager;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.InlineBlobStoreAccess;
import org.lilycms.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

public class BlobStoreTest {

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static IdGenerator idGenerator = new IdGeneratorImpl();
    private static AbstractTypeManager typeManager;
    private static HBaseRepository repository;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        typeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS());
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(HBASE_PROXY.getConf());
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess(); 
        SizeBasedBlobStoreAccessFactory factory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        factory.addBlobStoreAccess(50, inlineBlobStoreAccess);
        factory.addBlobStoreAccess(1024, hbaseBlobStoreAccess);
        repository = new HBaseRepository(typeManager, idGenerator, factory, HBASE_PROXY.getConf());
        repository.registerBlobStoreAccess(dfsBlobStoreAccess);
        repository.registerBlobStoreAccess(hbaseBlobStoreAccess);
        repository.registerBlobStoreAccess(inlineBlobStoreAccess);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repository.stop();
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreate() throws Exception {
        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = new Blob("aMimetype", (long)bytes.length, "testCreate");
        OutputStream outputStream = repository.getOutputStream(blob);
        outputStream.write(bytes);
        outputStream.close();
        
        InputStream inputStream = repository.getInputStream(blob);
        byte[] readBytes = new byte[blob.getSize().intValue()];
        inputStream.read(readBytes);
        inputStream.close();
        assertTrue(Arrays.equals(bytes, readBytes));
    }
    
    @Test
    public void testThreeSizes() throws Exception {
        Random random = new Random();
        byte[] small = new byte[10];
        random.nextBytes(small);
        byte[] medium = new byte[100];
        random.nextBytes(medium);
        byte[] large = new byte[2048];
        random.nextBytes(large);
        Blob smallBlob = new Blob("mime/small", (long)10, "small");
        Blob mediumBlob = new Blob("mime/medium", (long)100, "medium");
        Blob largeBlob = new Blob("mime/large", (long)2048, "large");
        OutputStream outputStream = repository.getOutputStream(smallBlob);
        outputStream.write(small);
        outputStream.close();
        outputStream = repository.getOutputStream(mediumBlob);
        outputStream.write(medium);
        outputStream.close();
        outputStream = repository.getOutputStream(largeBlob);
        outputStream.write(large);
        outputStream.close();

        InputStream inputStream = repository.getInputStream(smallBlob);
        byte[] readBytes = new byte[10];
        inputStream.read(readBytes);
        inputStream.close();
        assertTrue(Arrays.equals(small, readBytes));
        inputStream = repository.getInputStream(mediumBlob);
        readBytes = new byte[100];
        inputStream.read(readBytes);
        inputStream.close();
        assertTrue(Arrays.equals(medium, readBytes));
        inputStream = repository.getInputStream(largeBlob);
        readBytes = new byte[2048];
        inputStream.read(readBytes);
        inputStream.close();
        assertTrue(Arrays.equals(large, readBytes));
    }
    
    @Test
    public void testCreateRecordWithBlob() throws Exception {
        QName fieldName = new QName("test", "ablob");
        FieldType fieldType = typeManager.newFieldType(typeManager.getValueType("BLOB", false, false), fieldName, Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        RecordType recordType = typeManager.newRecordType(new QName(null, "testCreateRecordWithBlobRT"));
        FieldTypeEntry fieldTypeEntry = typeManager.newFieldTypeEntry(fieldType.getId(), true);
        recordType.addFieldTypeEntry(fieldTypeEntry);
        recordType = typeManager.createRecordType(recordType);
        Record record = repository.newRecord();
        record.setRecordType(recordType.getName(), null);

        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = new Blob("aMimetype", (long)bytes.length, "testCreate");
        OutputStream outputStream = repository.getOutputStream(blob);
        outputStream.write(bytes);
        outputStream.close();
        record.setField(fieldName, blob);
        record = repository.create(record);
        
        record = repository.read(record.getId());
        blob = (Blob)record.getField(fieldName);
        InputStream inputStream = repository.getInputStream(blob);
        byte[] readBytes = new byte[blob.getSize().intValue()];
        inputStream.read(readBytes);
        inputStream.close();
        assertTrue(Arrays.equals(bytes, readBytes));
    }
    
    @Test
    public void testReadBlobWithoutName() throws Exception {
        Blob blob = new Blob("aMimetype", (long)10, "aName");
        try {
            repository.getInputStream(blob);
            fail();
        } catch (BlobNotFoundException expected) {
        }
    }
    
    @Test
    public void testBadEncoding() throws Exception {
        Blob blob = new Blob("aMimetype", (long)10, "aName");
        blob.setValue(new byte[0]);
        try {
            repository.getInputStream(blob);
            fail();
        } catch (BlobException expected) {
        }
    }
    
    @Test
    public void testDelete() throws Exception {
        byte[] small = new byte[10];
        byte[] medium = new byte[100];
        byte[] large = new byte[2048];
        Blob smallBlob = new Blob("mime/small", (long)10, "small");
        OutputStream outputStream = repository.getOutputStream(smallBlob);
        outputStream.write(small);
        outputStream.close();
        Blob mediumBlob = new Blob("mime/medium", (long)100, "medium");
        outputStream = repository.getOutputStream(mediumBlob);
        outputStream.write(medium);
        outputStream.close();
        Blob largeBlob = new Blob("mime/large", (long)10, "large");
        outputStream = repository.getOutputStream(largeBlob);
        outputStream.write(large);
        outputStream.close();
        
        repository.delete(smallBlob);
        // TODO ok to ignore a delete of an inline blob? it will be deleted when the record is deleted 
        repository.getInputStream(smallBlob);
        repository.delete(mediumBlob);
        try {
            repository.getInputStream(smallBlob);
        } catch (BlobException expected) {
        }
        repository.delete(largeBlob);
        try {
            repository.getInputStream(smallBlob);
        } catch (BlobException expected) {
        }
    }
}
