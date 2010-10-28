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
package org.lilyproject.process.test;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.*;
import org.lilyproject.testfw.HBaseProxy;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;

public class LilyClientTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private final static KauriTestUtility KAURI_TEST_UTIL = new KauriTestUtility("../server/");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        HBASE_PROXY.start();

        KAURI_TEST_UTIL.createDefaultConf(HBASE_PROXY);
        KAURI_TEST_UTIL.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            KAURI_TEST_UTIL.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }

        try {
            HBASE_PROXY.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /**
     * Creates a record with a blob using a Repository obtained via LilyClient. This verifies
     * that the blobs work remotely and it was able to retrieve the blob stores config from
     * ZooKeeper.
     */
    @Test
    public void testBlob() throws Exception {
        LilyClient client = new LilyClient(HBASE_PROXY.getZkConnectString(), 10000);

        // Obtain a repository
        Repository repository = client.getRepository();

        String NS = "org.lilyproject.client.test";

        // Create a blob field type and record type
        TypeManager typeManager = repository.getTypeManager();
        ValueType blobType = typeManager.getValueType("BLOB", false, false);
        FieldType blobFieldType = typeManager.newFieldType(blobType, new QName(NS, "data"), Scope.VERSIONED);
        blobFieldType = typeManager.createFieldType(blobFieldType);

        RecordType recordType = typeManager.newRecordType(new QName(NS, "file"));
        recordType.addFieldTypeEntry(blobFieldType.getId(), true);
        recordType = typeManager.createRecordType(recordType);


        // Upload a blob that, based upon the current default config, should end up in HBase
        //  (> 5000 bytes and < 200000 bytes)
        byte[] data = makeBlobData(10000);
        Blob blob = new Blob("application/octet-stream", (long)data.length, null);
        OutputStream blobStream = repository.getOutputStream(blob);
        IOUtils.copy(new ByteArrayInputStream(data), blobStream);
        blobStream.close();
        assertTrue(blob.getValue() != null);

        // Create a record wit this blob
        Record record = repository.newRecord();
        record.setRecordType(new QName(NS, "file"));
        record.setField(new QName(NS, "data"), blob);
        record = repository.create(record);
    }

    private byte[] makeBlobData(int size) {
        return new byte[size];
    }

}
