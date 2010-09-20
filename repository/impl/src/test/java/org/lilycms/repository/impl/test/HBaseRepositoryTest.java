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
import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IMocksControl;
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
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.impl.RowLogConfigurationManager;
import org.lilycms.testfw.TestHelper;

public class HBaseRepositoryTest extends AbstractRepositoryTest {

    private static BlobStoreAccessFactory blobStoreAccessFactory;
    private static Configuration configuration;
    private static RowLogConfigurationManager rowLogConfigurationManager;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        configuration = HBASE_PROXY.getConf();
        typeManager = new HBaseTypeManager(idGenerator, configuration);
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory , configuration);
        rowLogConfigurationManager = new RowLogConfigurationManager(HBASE_PROXY.getZkConnectString());
        setupTypes();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
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
        IMocksControl control = EasyMock.createControl();
        RowLogMessageConsumer testConsumer = control.createMock(RowLogMessageConsumer.class); 

        testConsumer.getId();
        EasyMock.expectLastCall().andReturn(2).anyTimes();
        
        testConsumer.getMaxTries();
        EasyMock.expectLastCall().andReturn(5).anyTimes();
        
        final Capture<RowLogMessage> capturedMessage = new Capture<RowLogMessage>();
        testConsumer.processMessage(EasyMock.capture(capturedMessage));
        EasyMock.expectLastCall().andReturn(false);
        
        testConsumer.processMessage(EasyMock.isA(RowLogMessage.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            public Object answer() throws Throwable {
                Object[] currentArguments = EasyMock.getCurrentArguments();
                RowLogMessage rowLogMessage = (RowLogMessage)currentArguments[0];
                Assert.assertEquals(capturedMessage.getValue(), rowLogMessage);
                return true;
            }
        });
        
        testConsumer.processMessage(EasyMock.isA(RowLogMessage.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            public Object answer() throws Throwable {
                Object[] currentArguments = EasyMock.getCurrentArguments();
                RowLogMessage rowLogMessage = (RowLogMessage)currentArguments[0];
                Assert.assertFalse(capturedMessage.getValue().equals(rowLogMessage));
                return true;
            }
        });
        
        control.replay();
        RowLog wal = ((HBaseRepository)repository).getWal();
        wal.registerConsumer(testConsumer);
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record = repository.create(record);
        record.setField(fieldType1.getName(), "value2");
        record = repository.update(record);

        assertEquals("value2", record.getField(fieldType1.getName()));

        assertEquals(record, repository.read(record.getId()));
        wal.unRegisterConsumer(testConsumer);
        control.verify();
    }
}
