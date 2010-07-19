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


import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilycms.testfw.TestHelper;

public class HBaseRepositoryTest extends AbstractRepositoryTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        typeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS());
        BlobStoreAccessFactory blobStoreOutputStreamFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreOutputStreamFactory , HBASE_PROXY.getConf());
        setupTypes();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        ((HBaseRepository)repository).stop();
        HBASE_PROXY.stop();
    }

    @Test
    public void testFieldTypeCacheInitialization() throws Exception {
        TypeManager newTypeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        assertEquals(fieldType1, newTypeManager.getFieldTypeByName(fieldType1.getName()));
    }
}
