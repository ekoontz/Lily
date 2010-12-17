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
package org.lilyproject.repository.impl.test;

import static org.easymock.EasyMock.createControl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.InvalidRecordException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;
import org.lilyproject.rowlog.impl.MessageQueueFeeder;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogProcessorImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

public abstract class AbstractRepositoryTest {

    protected static final HBaseProxy HBASE_PROXY = new HBaseProxy();
    protected static RowLog messageQueue;
    protected static IdGenerator idGenerator = new IdGeneratorImpl();
    protected static TypeManager typeManager;
    protected static Repository repository;
    protected static FieldType fieldType1;
    private static FieldType fieldType1B;
    private static FieldType fieldType2;
    private static FieldType fieldType3;
    private static FieldType fieldType4;
    private static FieldType fieldType5;
    private static FieldType fieldType6;
    private static FieldType lastVTag;
    protected static RecordType recordType1;
    private static RecordType recordType1B;
    private static RecordType recordType2;
    private static RecordType recordType3;
    protected static RowLogConfigurationManagerImpl rowLogConfigurationManager;
    protected static RowLogProcessorImpl messageQueueProcessor;
    protected static Configuration configuration;
    protected static RowLog wal;
    protected static ZooKeeperItf zooKeeper;
    protected static boolean avro = false;
    protected static HBaseTableFactory hbaseTableFactory;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    protected static void setupTypes() throws Exception {
        setupFieldTypes();
        setupRecordTypes();
    }
    
    protected static void setupWal() throws Exception {
        rowLogConfigurationManager.addRowLog("WAL", new RowLogConfig(10000L, true, false, 100L, 5000L));
        wal = new RowLogImpl("WAL", hbaseTableFactory.getRecordTable(), RecordCf.WAL_PAYLOAD.bytes, RecordCf.WAL_STATE.bytes, rowLogConfigurationManager);
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal, 100);
        wal.registerShard(walShard);
    }

    private static void setupFieldTypes() throws Exception {
        String namespace = "/test/repository";
        fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING", false, false), new QName(namespace, "field1"), Scope.NON_VERSIONED));
        fieldType1B = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING", false, false), new QName(namespace, "field1B"), Scope.NON_VERSIONED));
        fieldType2 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("INTEGER", false, false), new QName(namespace, "field2"), Scope.VERSIONED));
        fieldType3 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("BOOLEAN", false, false), new QName(namespace, "field3"),
                Scope.VERSIONED_MUTABLE));

        fieldType4 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("INTEGER", false, false), new QName(namespace, "field4"), Scope.NON_VERSIONED));
        fieldType5 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("BOOLEAN", false, false), new QName(namespace, "field5"), Scope.VERSIONED));
        fieldType6 = typeManager.createFieldType(typeManager
                .newFieldType(typeManager.getValueType("STRING", false, false), new QName(namespace, "field6"), Scope.VERSIONED_MUTABLE));
        
        lastVTag = VersionTag.createLastVTagType(typeManager);
    }

    private static void setupRecordTypes() throws Exception {
        recordType1 = typeManager.newRecordType(new QName("test", "RT1"));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(lastVTag.getId(), false));
        recordType1 = typeManager.createRecordType(recordType1);

        recordType1B = recordType1.clone();
        recordType1B.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1B.getId(), false));
        recordType1B = typeManager.updateRecordType(recordType1B);

        recordType2 = typeManager.newRecordType(new QName("test", "RT2"));

        recordType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType4.getId(), false));
        recordType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType5.getId(), false));
        recordType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType6.getId(), false));
        recordType2 = typeManager.createRecordType(recordType2);
        
        recordType3 = typeManager.newRecordType(new QName("test", "RT3"));
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType3 = typeManager.createRecordType(recordType3);
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        recordType3 = typeManager.updateRecordType(recordType3);
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), true));
        recordType3 = typeManager.updateRecordType(recordType3);
        recordType3.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType6.getId(), false));
        recordType3 = typeManager.updateRecordType(recordType3);
    }

    protected static void setupRowLogConfigurationManager(ZooKeeperItf zooKeeper) throws Exception {
        rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zooKeeper);
    }
    
    protected static void setupMessageQueue() throws Exception {
        
        rowLogConfigurationManager.addRowLog("MQ", new RowLogConfig(10000L, false, true, 100L, 0L));
        rowLogConfigurationManager.addSubscription("WAL", "MQFeeder", Type.VM, 3, 1);
        messageQueue = new RowLogImpl("MQ", hbaseTableFactory.getRecordTable(), RecordCf.MQ_PAYLOAD.bytes,
                RecordCf.MQ_STATE.bytes, rowLogConfigurationManager);
        messageQueue.registerShard(new RowLogShardImpl("MQS1", configuration, messageQueue, 100));

        RowLogMessageListenerMapping listenerClassMapping = RowLogMessageListenerMapping.INSTANCE;
        listenerClassMapping.put("MQFeeder", new MessageQueueFeeder(messageQueue));
    }

    protected static void setupMessageQueueProcessor() throws RowLogException, KeeperException, InterruptedException {
        messageQueueProcessor = new RowLogProcessorImpl(messageQueue, rowLogConfigurationManager);
        messageQueueProcessor.start();
    }

    @Test
    public void testRecordCreateWithoutRecordType() throws Exception {
        IMocksControl control = createControl();
        control.replay();
        Record record = repository.newRecord(idGenerator.newRecordId());
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            record = repository.create(record);
            fail();
        } catch (InvalidRecordException expected) {
        }
        control.verify();
    }

    @Test
    public void testEmptyRecordCreate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            record = repository.create(record);
            fail();
        } catch (InvalidRecordException expected) {
        }
    }

    @Test
    public void testCreate() throws Exception {
        IMocksControl control = createControl();
        control.replay();
        Record createdRecord = createDefaultRecord();

        assertEquals(Long.valueOf(1), createdRecord.getVersion());
        assertEquals("value1", createdRecord.getField(fieldType1.getName()));
        assertEquals(123, createdRecord.getField(fieldType2.getName()));
        assertTrue((Boolean) createdRecord.getField(fieldType3.getName()));
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName());
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion());
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(Long.valueOf(1), createdRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        assertEquals(createdRecord, repository.read(createdRecord.getId()));
        control.verify();
    }
    
    @Test
    public void testCreateNoVersions() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        
        record = repository.create(record);
        assertEquals(null, record.getVersion());
    }

    protected Record createDefaultRecord() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "value1");
        record.setField(fieldType2.getName(), 123);
        record.setField(fieldType3.getName(), true);
        return repository.create(record);
    }
    
    @Test
    public void testCreateWithNonExistingRecordTypeFails() throws Exception {
        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(new QName("foo", "bar"));
        record.setField(fieldType1.getName(), "value1");
        try {
            if (avro)
                System.out.println("Expecting RecordTypeNotFoundException");
            repository.create(record);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testCreateUsesLatestRecordType() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName());
        record.setField(fieldType1.getName(), "value1");
        Record createdRecord = repository.create(record);
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName());
        assertEquals(Long.valueOf(2), createdRecord.getRecordTypeVersion());
        assertEquals(recordType1.getName(), createdRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(Long.valueOf(2), createdRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertNull(createdRecord.getRecordTypeName(Scope.VERSIONED));
        assertNull(createdRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertNull(createdRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertNull(createdRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        assertEquals(createdRecord, repository.read(createdRecord.getId()));
    }
    
    @Test
    public void testCreateVariant() throws Exception {
        Record record = createDefaultRecord();

        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dimension1", "dimval1");
        Record variant = repository.newRecord(idGenerator.newRecordId(record.getId(), variantProperties));
        variant.setRecordType(recordType1.getName());
        variant.setField(fieldType1.getName(), "value2");
        variant.setField(fieldType2.getName(), 567);
        variant.setField(fieldType3.getName(), false);

        Record createdVariant = repository.create(variant);

        assertEquals(Long.valueOf(1), createdVariant.getVersion());
        assertEquals("value2", createdVariant.getField(fieldType1.getName()));
        assertEquals(567, createdVariant.getField(fieldType2.getName()));
        assertFalse((Boolean) createdVariant.getField(fieldType3.getName()));

        assertEquals(createdVariant, repository.read(variant.getId()));

        Set<RecordId> variants = repository.getVariants(record.getId());
        assertEquals(2, variants.size());
        assertTrue(variants.contains(record.getId()));
        assertTrue(variants.contains(createdVariant.getId()));
    }

    @Test
    public void testUpdate() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(2), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(fieldType1.getName()));
        assertEquals(789, updatedRecord.getField(fieldType2.getName()));
        assertEquals(false, updatedRecord.getField(fieldType3.getName()));

        assertEquals(updatedRecord, repository.read(record.getId()));
    }
    
    @Test
    public void testUpdateWithoutRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(record.getRecordTypeName(), updatedRecord.getRecordTypeName());
        assertEquals(Long.valueOf(2), updatedRecord.getRecordTypeVersion());
        
        assertEquals(Long.valueOf(2), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(fieldType1.getName()));
        assertEquals(789, updatedRecord.getField(fieldType2.getName()));
        assertEquals(false, updatedRecord.getField(fieldType3.getName()));

        assertEquals(updatedRecord, repository.read(record.getId()));
    }

    @Test
    public void testUpdateOnlyOneField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType1.getName(), "value2");

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        assertEquals("value2", updatedRecord.getField(fieldType1.getName()));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        Record readRecord = repository.read(record.getId());
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testEmptyUpdate() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            updatedRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        assertEquals(record, repository.read(record.getId()));
    }

    @Test
    public void testIdempotentUpdate() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());
        assertEquals("value1", updatedRecord.getField(fieldType1.getName()));
        assertEquals(123, updatedRecord.getField(fieldType2.getName()));
        assertEquals(true, updatedRecord.getField(fieldType3.getName()));

        assertEquals(record, repository.read(record.getId()));
    }

    @Test
    public void testUpdateIgnoresVersion() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setVersion(Long.valueOf(99));
        updateRecord.setField(fieldType1.getName(), "value2");

        Record updatedRecord = repository.update(updateRecord);

        assertEquals(Long.valueOf(1), updatedRecord.getVersion());

        assertEquals(updatedRecord, repository.read(record.getId()));
    }

    @Test
    public void testUpdateNonVersionable() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName());
        updateRecord.setField(fieldType1.getName(), "aNewValue");
        repository.update(updateRecord);

        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("aNewValue", readRecord.getField(fieldType1.getName()));
    }

    @Test
    public void testReadOlderVersions() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        // This update will use the latest version of the RecordType
        // I.e. version2 of recordType1 instead of version 1
        repository.update(updateRecord);

        record.setRecordType(recordType1B.getName(), recordType1B.getVersion());
        record.setField(fieldType1.getName(), "value2");
        record.setField(lastVTag.getName(), 2L);
        assertEquals(record, repository.read(record.getId(), 1L));
        
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(record.getId(), 0L);
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }

    @Test
    public void testReadAllVersions() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        repository.update(updateRecord);
        
        List<Record> list = repository.readVersions(record.getId(), 1L, 2L, null);
        assertEquals(2, list.size());
        assertTrue(list.contains(repository.read(record.getId(), 1L)));
        assertTrue(list.contains(repository.read(record.getId(), 2L)));
    }

    @Test
    public void testReadVersionsWideBoundaries() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        repository.update(updateRecord);
        
        List<Record> list = repository.readVersions(record.getId(), 0L, 5L, null);
        assertEquals(2, list.size());
        assertTrue(list.contains(repository.read(record.getId(), 1L)));
        assertTrue(list.contains(repository.read(record.getId(), 2L)));
    }
    
    @Test
    public void testReadVersionsNarrowBoundaries() throws Exception {
        Record record = createDefaultRecord();

        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord);

        updateRecord = record.clone();
        updateRecord.setField(fieldType2.getName(), 790);
        repository.update(updateRecord);
        
        updateRecord = record.clone();
        updateRecord.setField(fieldType2.getName(), 791);
        repository.update(updateRecord);

        List<Record> list = repository.readVersions(record.getId(), 2L, 3L, null);
        assertEquals(2, list.size());
        assertTrue(list.contains(repository.read(record.getId(), 2L)));
        assertTrue(list.contains(repository.read(record.getId(), 3L)));
    }
    
    @Test
    public void testReadNonExistingRecord() throws Exception {
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(idGenerator.newRecordId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }

    @Test
    public void testReadTooRecentRecord() throws Exception {
        Record record = createDefaultRecord();
        try {
            if (avro)
                System.out.println("Expecting VersionNotFoundException");
            repository.read(record.getId(), Long.valueOf(2));
            fail();
        } catch (VersionNotFoundException expected) {
        }
    }

    @Test
    public void testReadSpecificFields() throws Exception {
        Record record = createDefaultRecord();
        Record readRecord = repository.read(record.getId(), Arrays.asList(new QName[] { lastVTag.getName(), fieldType1.getName(), fieldType2.getName(), fieldType3.getName() }));
        assertEquals(repository.read(record.getId()), readRecord);
    }

    @Test
    public void testUpdateWithNewRecordTypeVersion() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1B.getName(), recordType1B.getVersion());
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);

        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        Record recordV1 = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(recordType1B.getName(), recordV1.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), recordV1.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), recordV1.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getVersion(), recordV1.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), recordV1.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1.getVersion(), recordV1.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), recordV1.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1.getVersion(), recordV1.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testUpdateWithNewRecordTypeVersionOnlyOneFieldUpdated() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1B.getName(), recordType1B.getVersion());
        updateRecord.setField(fieldType2.getName(), 789);

        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));

        Record readRecord = repository.read(record.getId());
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType1B.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getVersion(), readRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType1B.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1B.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType1.getVersion(), readRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testUpdateWithNewRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateRecord.setField(fieldType4.getName(), 1024);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");

        Record updatedRecord = repository.update(updateRecord);
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName());
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion());
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion(Scope.NON_VERSIONED));
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED));
        assertEquals(recordType2.getName(), updatedRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
        assertEquals(recordType2.getVersion(), updatedRecord.getRecordTypeVersion(Scope.VERSIONED_MUTABLE));

        assertEquals(4, updatedRecord.getFields().size());

        Record readRecord = repository.read(record.getId());
        // Nothing got deleted
        assertEquals(7, readRecord.getFields().size());
        assertEquals("value1", readRecord.getField(fieldType1.getName()));
        assertEquals(1024, readRecord.getField(fieldType4.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        assertFalse((Boolean) readRecord.getField(fieldType5.getName()));
        assertTrue((Boolean) readRecord.getField(fieldType3.getName()));
        assertEquals("value2", readRecord.getField(fieldType6.getName()));
        assertEquals(2L, readRecord.getField(lastVTag.getName()));
    }

    @Test
    public void testDeleteField() throws Exception {
        Record record = createDefaultRecord();
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(record.getRecordTypeName());
        deleteRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType1.getName(), fieldType2.getName(), fieldType3.getName()}));

        repository.update(deleteRecord);
        Record readRecord = repository.read(record.getId());
        assertEquals(1, readRecord.getFields().size());
        assertEquals(2L, readRecord.getField(lastVTag.getName()));
    }

    @Test
    public void testDeleteFieldFollowedBySet() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record = repository.create(record);

        // Delete the field
        record.delete(fieldType1.getName(), true);
        record = repository.update(record);
        assertFalse(record.getFieldsToDelete().contains(fieldType1.getName()));

        // Set the field again
        record.setField(fieldType1.getName(), "hello");
        record = repository.update(record);
        assertEquals("hello", record.getField(fieldType1.getName()));

        // Check it also there after a fresh read
        record = repository.read(record.getId());
        assertEquals("hello", record.getField(fieldType1.getName()));

        // Calling delete field followed by set field should remove it from the deleted fields
        record.delete(fieldType1.getName(), true);
        assertTrue(record.getFieldsToDelete().contains(fieldType1.getName()));
        record.setField(fieldType1.getName(), "hello");
        assertFalse(record.getFieldsToDelete().contains(fieldType1.getName()));
    }

    @Test
    public void testDeleteFieldsNoLongerInRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateRecord.setField(fieldType4.getName(), 2222);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");

        repository.update(updateRecord);

        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(recordType1.getName(), recordType1.getVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType1.getName() }));
        repository.update(deleteRecord);

        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        assertEquals(6, readRecord.getFields().size());
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException"); 
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        assertEquals("value2", readRecord.getField(fieldType6.getName()));
        assertEquals(2222, readRecord.getField(fieldType4.getName()));

        deleteRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType2.getName(), fieldType3.getName() }));
        repository.update(deleteRecord);

        readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(3), readRecord.getVersion());
        assertEquals(4, readRecord.getFields().size());
        assertEquals(2222, readRecord.getField(fieldType4.getName()));
        assertEquals(false, readRecord.getField(fieldType5.getName()));
        assertEquals("value2", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testDeleteFieldTwice() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateRecord.setField(fieldType4.getName(), 2222);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");

        repository.update(updateRecord);

        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(recordType1.getName(), recordType1.getVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType1.getName() }));
        repository.update(deleteRecord);
        repository.update(deleteRecord);
    }

    @Test
    public void testUpdateAfterDelete() throws Exception {
        Record record = createDefaultRecord();
        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType2.getName() }));
        repository.update(deleteRecord);

        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType2.getName(), 3333);
        repository.update(updateRecord);

        // Read version 3
        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(3), readRecord.getVersion());
        assertEquals(3333, readRecord.getField(fieldType2.getName()));

        // Read version 2
        readRecord = repository.read(record.getId(), Long.valueOf(2));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        // Read version 1
        readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
    }

    @Test
    public void testDeleteNonVersionableFieldAndUpdateVersionableField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType2.getName(), 999);
        updateRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType1.getName() }));
        repository.update(updateRecord);

        Record readRecord = repository.read(record.getId());
        assertEquals(999, readRecord.getField(fieldType2.getName()));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId(), Long.valueOf(1));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType1.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

    }

    @Test
    public void testUpdateAndDeleteSameField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = repository.newRecord(record.getId());
        updateRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType2.getName() }));
        repository.update(updateRecord);

        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            repository.read(record.getId()).getField(fieldType2.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
    }

    @Test
    public void testDeleteRecord() throws Exception {
        Record record = createDefaultRecord();
        repository.delete(record.getId());
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.read(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.update(record);
            fail();
        } catch (RecordNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting RecordNotFoundException");
            repository.delete(record.getId());
            fail();
        } catch (RecordNotFoundException expected) {
        }
    }
    
    @Test
    public void TestDeleteRecordCleansUpDataBeforeRecreate() throws Exception {
        // We will not allow recreation after a delete.
        // A record will be marked as deleted, creating it again will cause a next version to be created
        
//        Record record = createDefaultRecord();
//        RecordId recordId = record.getId();
//        repository.delete(recordId);
//        
//     // Work around HBASE-2256
////        HBASE_PROXY.majorCompact("recordTable", new String[] {"VSCF", "VCF", "VMCF"});
//
//        record = repository.newRecord(recordId);
//        record.setRecordType(recordType2.getName(), recordType2.getVersion());
//        record.setField(fieldType4.getName(), 555);
//        record.setField(fieldType5.getName(), false);
//        record.setField(fieldType6.getName(), "zzz");
//        repository.create(record);
//        Record readRecord = repository.read(recordId);
//        assertEquals(Long.valueOf(1), readRecord.getVersion());
//        try {
//            readRecord.getField(fieldType1.getName());
//            fail();
//        } catch (FieldNotFoundException expected) {
//        }
//        try {
//            readRecord.getField(fieldType2.getName());
//            fail();
//        } catch (FieldNotFoundException expected) {
//        }
//        try {
//            readRecord.getField(fieldType3.getName());
//            fail();
//        } catch (FieldNotFoundException expected) {
//        }
//        
//        assertEquals(555, readRecord.getField(fieldType4.getName()));
//        assertFalse((Boolean)readRecord.getField(fieldType5.getName()));
//        assertEquals("zzz", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testUpdateMutableField() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType2.getName(), recordType2.getVersion());
        record.setField(fieldType4.getName(), 123);
        record.setField(fieldType5.getName(), true);
        record.setField(fieldType6.getName(), "value1");
        record = repository.create(record);
        
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType4.getName(), 456);
        updateRecord.setField(fieldType5.getName(), false);
        updateRecord.setField(fieldType6.getName(), "value2");
        repository.update(updateRecord);

        // Read version 1
        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(456, readRecord.getField(fieldType4.getName()));
        assertEquals(true, readRecord.getField(fieldType5.getName()));
        assertEquals("value1", readRecord.getField(fieldType6.getName()));
        
        // Update mutable version 1
        Record mutableRecord = repository.newRecord(record.getId());
        mutableRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        mutableRecord.setField(fieldType6.getName(), "value3");
        mutableRecord.setVersion(1L);
        mutableRecord = repository.update(mutableRecord, true, false);
        
        // Read version 1 again
        readRecord = repository.read(record.getId(), 1L);
        assertEquals(456, readRecord.getField(fieldType4.getName()));
        assertEquals(true, readRecord.getField(fieldType5.getName()));
        assertEquals("value3", readRecord.getField(fieldType6.getName()));
        
        // Update mutable version 2
        mutableRecord.setVersion(2L);
        mutableRecord.setField(fieldType6.getName(), "value4");
        mutableRecord = repository.update(mutableRecord, true, false);

        // Read version 2
        readRecord = repository.read(record.getId(), 2L);
        assertEquals(456, readRecord.getField(fieldType4.getName()));
        assertEquals(false, readRecord.getField(fieldType5.getName()));
        assertEquals("value4", readRecord.getField(fieldType6.getName()));
    }
    
    @Test
    public void testUpdateMutableFieldWithNewRecordType() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord);

        Record updateMutableRecord = repository.newRecord(record.getId());
        updateMutableRecord.setVersion(Long.valueOf(1));
        updateMutableRecord.setRecordType(recordType2.getName(), recordType2.getVersion());
        updateMutableRecord.setField(fieldType4.getName(), 888);
        updateMutableRecord.setField(fieldType5.getName(), false);
        updateMutableRecord.setField(fieldType6.getName(), "value3");

        assertEquals(Long.valueOf(1), repository.update(updateMutableRecord, true, false).getVersion());

        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
        // Only the mutable fields got updated
        assertEquals("value3", readRecord.getField(fieldType6.getName()));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType4.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType5.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName());
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType2.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));

        readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(2), readRecord.getVersion());
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(789, readRecord.getField(fieldType2.getName()));
        assertEquals(false, readRecord.getField(fieldType3.getName()));
        assertEquals("value3", readRecord.getField(fieldType6.getName()));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName());
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.NON_VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED));
        assertEquals(recordType1.getName(), readRecord.getRecordTypeName(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testUpdateMutableFieldCopiesValueToNext() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord = repository.update(updateRecord); // Leave mutable field
        // same on first update

        updateRecord.setField(fieldType3.getName(), false);
        updateRecord = repository.update(updateRecord);

        Record readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
        
        updateRecord  = repository.newRecord(record.getId());
        updateRecord.setRecordType(recordType1.getName(), recordType1.getVersion());
        updateRecord.setField(fieldType3.getName(), false);
        updateRecord.setVersion(1L);
        
        repository.update(updateRecord, true, false);
        
        readRecord = repository.read(record.getId(), Long.valueOf(1));
        assertFalse((Boolean)readRecord.getField(fieldType3.getName()));
        readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertTrue((Boolean)readRecord.getField(fieldType3.getName()));
        readRecord = repository.read(record.getId(), Long.valueOf(3));
        assertFalse((Boolean)readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testDeleteMutableField() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord.setField(fieldType3.getName(), false);
        repository.update(updateRecord);

        Record deleteRecord = repository.newRecord(record.getId());
        deleteRecord.setVersion(Long.valueOf(1));
        deleteRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        deleteRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType1.getName(), fieldType2.getName(), fieldType3.getName() }));

        repository.update(deleteRecord, true, false);

        Record readRecord = repository.read(record.getId(), Long.valueOf(1));
        // The non-mutable fields were ignored
        assertEquals("value2", readRecord.getField(fieldType1.getName()));
        assertEquals(123, readRecord.getField(fieldType2.getName()));
        try {
            // The mutable field got deleted
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId());
        assertEquals(false, readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testDeleteMutableFieldCopiesValueToNext() throws Exception {
        Record record = createDefaultRecord();
        Record updateRecord = record.clone();
        updateRecord.setField(fieldType1.getName(), "value2");
        updateRecord.setField(fieldType2.getName(), 789);
        updateRecord = repository.update(updateRecord); // Leave mutable field
        // same on first update

        updateRecord.setField(fieldType3.getName(), false);
        updateRecord = repository.update(updateRecord);

        Record readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
        
        Record deleteMutableFieldRecord = repository.newRecord(record.getId());
        deleteMutableFieldRecord.setVersion(Long.valueOf(1));
        deleteMutableFieldRecord.setRecordType(record.getRecordTypeName(), record.getRecordTypeVersion());
        deleteMutableFieldRecord.addFieldsToDelete(Arrays.asList(new QName[] { fieldType3.getName() }));

        repository.update(deleteMutableFieldRecord, true, false);

        readRecord = repository.read(record.getId(), Long.valueOf(1));
        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            readRecord.getField(fieldType3.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }

        readRecord = repository.read(record.getId(), Long.valueOf(2));
        assertEquals(true, readRecord.getField(fieldType3.getName()));

        readRecord = repository.read(record.getId());
        assertEquals(false, readRecord.getField(fieldType3.getName()));
    }

    @Test
    public void testMixinLatestVersion() throws Exception {
        RecordType recordType4 = typeManager.newRecordType(new QName("test", "RT4"));
        recordType4.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType6.getId(), false));
        recordType4.addMixin(recordType1.getId()); // In fact recordType1B should be taken as Mixin
        recordType4 = typeManager.createRecordType(recordType4);

        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType4.getName(), recordType4.getVersion());
        record.setField(fieldType1.getName(), "foo");
        record.setField(fieldType2.getName(), 555);
        record.setField(fieldType3.getName(), true);
        record.setField(fieldType1B.getName(), "fromLatestMixinRecordTypeVersion");
        record.setField(fieldType6.getName(), "bar");
        record = repository.create(record);

        Record readRecord = repository.read(record.getId());
        assertEquals(Long.valueOf(1), readRecord.getVersion());
        assertEquals("foo", readRecord.getField(fieldType1.getName()));
        assertEquals(555, readRecord.getField(fieldType2.getName()));
        assertEquals(true, readRecord.getField(fieldType3.getName()));
        assertEquals("fromLatestMixinRecordTypeVersion", readRecord.getField(fieldType1B.getName()));
        assertEquals("bar", readRecord.getField(fieldType6.getName()));
    }

    @Test
    public void testNonVersionedToVersioned() throws Exception {
        // Create a record with only a versioned and non-versioned field
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record.setField(fieldType2.getName(), new Integer(4));
        record = repository.create(record);

        // Try to read the created version
        record = repository.read(record.getId(), 1L);
    }

    @Test
    public void testIdRecord() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record.setField(fieldType2.getName(), new Integer(4));
        record = repository.create(record);

        IdRecord idRecord = repository.readWithIds(record.getId(), null, null);
        assertEquals("hello", idRecord.getField(fieldType1.getId()));
        assertTrue(idRecord.hasField(fieldType1.getId()));
        assertEquals(new Integer(4), idRecord.getField(fieldType2.getId()));
        assertTrue(idRecord.hasField(fieldType2.getId()));

        Map<String, Object> fields = idRecord.getFieldsById();
        assertEquals(3, fields.size());
        assertEquals("hello", fields.get(fieldType1.getId()));
        assertEquals(new Integer(4), fields.get(fieldType2.getId()));

        assertEquals(record, idRecord.getRecord());
    }

    @Test
    public void testVersionNumbers() throws Exception {
        // Create a record without versioned fields, the record will be without versions
        Record record = repository.newRecord();
        record.setRecordType(recordType1.getName(), recordType1.getVersion());
        record.setField(fieldType1.getName(), "hello");
        record = repository.create(record);

        // Check the version is null
        assertEquals(null, record.getVersion());

        // Check version number stays null after additional update
        record.setField(fieldType1.getName(), "hello2");
        repository.update(record);
        record = repository.read(record.getId());
        assertEquals(null, record.getVersion());

        // add a versioned field to the record
        record.setField(fieldType2.getName(), new Integer(4));
        record = repository.update(record);
        assertEquals(new Long(1), record.getVersion());

        // Verify the last version number after a fresh record read
        record = repository.read(record.getId());
        assertEquals(new Long(1), record.getVersion());

        // Read specific version
        record = repository.read(record.getId(), 1L);
        assertEquals(new Long(1), record.getVersion());
        assertTrue(record.hasField(fieldType2.getName()));
        assertEquals(3, record.getFields().size());

        try {
            if (avro)
                System.out.println("Expecting VersionNotFoundException");
            record = repository.read(record.getId(), 2L);
            fail("expected exception");
        } catch (VersionNotFoundException e) {
            // expected
        }
    }
    
    @Test
    public void testLastVTagNotDefinedInRecordType() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType2.getName());
        record.setField(fieldType5.getName(), false);
        record = repository.create(record);

        try {
            if (avro)
                System.out.println("Expecting FieldNotFoundException");
            record.getField(lastVTag.getName());
            fail();
        } catch (FieldNotFoundException expected) {
        }
        
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType2.getName());
        record.setField(fieldType5.getName(), true);
        record.setField(lastVTag.getName(), 3L);  // The given value will be ignored
        record = repository.update(record);
        
        assertEquals(Long.valueOf(2), record.getField(lastVTag.getName()));
        
        record = repository.newRecord(record.getId());
        record.setField(fieldType5.getName(), false);
        record = repository.update(record);
        
        assertEquals(Long.valueOf(3), record.getField(lastVTag.getName()));

        record = repository.newRecord(record.getId());
        record.setField(lastVTag.getName(), 2L);
        record = repository.update(record);
        // The last version tag is always updated by the system, even if its the only field set by the user to be updated
        assertEquals(Long.valueOf(3), record.getField(lastVTag.getName()));
        
        record = repository.newRecord(record.getId());
        record.addFieldsToDelete(Arrays.asList(new QName[]{lastVTag.getName(), fieldType5.getName()}));
        record = repository.update(record);
        // The record still has a version even though there are no longer any versioned fields on it
        assertEquals(Long.valueOf(4), record.getField(lastVTag.getName())); 
    }
    
    @Test
    public void testValidateCreate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 1L);
        record.setField(fieldType2.getName(), 123);
        repository.create(record);
        
        record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType2.getName(), 123);
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.create(record);
            fail();
        } catch (InvalidRecordException expected) {
        }
        
        record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType1.getName(), "abc");
        record.setField(fieldType2.getName(), 123);
    }
    
    @Test
    public void testValidateUpdate() throws Exception {
        Record record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 1L);
        record.setField(fieldType2.getName(), 123);
        record = repository.create(record);
        
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType2.getName(), 567);
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.update(record, false, false);
            fail();
        } catch (InvalidRecordException expected) {
        }
        
        record.setField(fieldType1.getName(), "abc");
        repository.update(record, false, false);
    }

    @Test
    public void testValidateMutableUpdate() throws Exception {
        // Nothing mandatory
        Record record = repository.newRecord();
        record.setRecordType(recordType3.getName(), 1L);
        record.setField(fieldType2.getName(), 123);
        record = repository.create(record);
        
        // Non-versioned field1 mandatory
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType1.getName(), "abc");
        repository.update(record, false, false); // record version 1
        
        // Mutable field3 mandatory
        record.setRecordType(recordType3.getName(), 3L);
        record.setField(fieldType1.getName(), "efg");
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.update(record, false, false);
            fail();
        } catch (InvalidRecordException expected) {
        }
        
        // Mutable field3 not mandatory
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType3.getName(), 2L);
        record.setField(fieldType3.getName(), true);
        repository.update(record, false, false); // record version 2
        
        // Mutable field update of record version 1 with field3 mandatory
        // Field3 already exists, but in record version 2 not version 1 
        record = repository.newRecord(record.getId());
        record.setRecordType(recordType3.getName(), 4L);
        record.setField(fieldType6.getName(), "zzz");
        record.setVersion(1L);
        try {
            if (avro)
                System.out.println("Expecting InvalidRecordException");
            repository.update(record, true, false);
            fail();
        } catch (InvalidRecordException expected) {
        }
        record.setField(fieldType3.getName(), false);
        repository.update(record, true, false);
    }
}
