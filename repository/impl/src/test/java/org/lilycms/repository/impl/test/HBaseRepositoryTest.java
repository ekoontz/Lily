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

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilycms.rowlog.api.SubscriptionContext.Type;
import org.lilycms.rowlog.impl.ListenerClassMapping;
import org.lilycms.testfw.TestHelper;

public class HBaseRepositoryTest extends AbstractRepositoryTest {

    private static BlobStoreAccessFactory blobStoreAccessFactory;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        configuration = HBASE_PROXY.getConf();
        typeManager = new HBaseTypeManager(idGenerator, configuration);
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory , configuration);

        setupTypes();
        setupMessageQueue();
        setupMessageQueueProcessor();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        messageQueueProcessor.stop();
        ((HBaseRepository)repository).stop();
        rowLogConfigurationManager.stop();
        HBASE_PROXY.stop();
    }

    @Test
    public void testFieldTypeCacheInitialization() throws Exception {
        TypeManager newTypeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        assertEquals(fieldType1, newTypeManager.getFieldTypeByName(fieldType1.getName()));
    }
    
    @Test
    public void testUpdateProcessesRemainingMessages() throws Exception {
        HBaseRepositoryTestConsumer.reset();
        ListenerClassMapping.INSTANCE.put("TestSubscription", HBaseRepositoryTestConsumer.class.getName());
        rowLogConfigurationManager.addSubscription("WAL", "TestSubscription", Type.VM, 3, 2);
        
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record = repository.create(record);
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record);

        assertEquals("value2", record.getField(fieldType1.getName()));

        assertEquals(record, repository.read(record.getId()));
        rowLogConfigurationManager.removeSubscription("WAL", "TestSubscription");
        ListenerClassMapping.INSTANCE.remove("TestSubscription");
    }
    
    
}
