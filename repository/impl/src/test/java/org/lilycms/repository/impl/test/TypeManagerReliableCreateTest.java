package org.lilycms.repository.impl.test;


import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.FieldTypeExistsException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.hbase.LocalHTable;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.StateWatchingZooKeeper;

public class TypeManagerReliableCreateTest {

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static final byte[] NON_VERSIONED_COLUMN_FAMILY = Bytes.toBytes("NVCF");
    private static final byte[] CONCURRENT_COUNTER_COLUMN_NAME = Bytes.toBytes("$cc");
    private static ValueType valueType;
    private static TypeManager basicTypeManager;
    private static StateWatchingZooKeeper zooKeeper;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);
        basicTypeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper);
        valueType = basicTypeManager.getValueType("LONG", false, false);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        HBASE_PROXY.cleanTables();
    }

    @Test
    public void testConcurrentRecordCreate() throws Exception {
        final HTableInterface typeTable = new LocalHTable(HBASE_PROXY.getConf(), Bytes.toBytes("typeTable")) {
            @Override
            public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                    throws IOException {
                long incrementColumnValue = super.incrementColumnValue(row, family, qualifier, amount);
                try {
                    basicTypeManager.createRecordType(basicTypeManager.newRecordType(new QName("NS", "name")));
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                } 
                return incrementColumnValue;
            }
        };
        
        TypeManager typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper) {
            @Override
            protected HTableInterface getTypeTable() {
                return typeTable;
            }
        };
        try {
            typeManager.createRecordType(typeManager.newRecordType(new QName("NS", "name")));
            fail();
        } catch (TypeException expected) {
        } catch (RecordTypeExistsException expected) {
            // This will be thrown when the cache of the typeManager was updated as a consequence of the update on basicTypeManager
            // Through ZooKeeper the cache will have been marked as invalidated
        }
    }
    
    @Test
    public void testConcurrentRecordUpdate() throws Exception {
        final HTableInterface typeTable = new LocalHTable(HBASE_PROXY.getConf(), Bytes.toBytes("typeTable")) {
            @Override
            public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                    throws IOException {
                long incrementColumnValue = super.incrementColumnValue(row, family, qualifier, amount);
                try {
                    RecordType recordType = basicTypeManager.getRecordTypeByName(new QName("NS", "name1"), null);
                    recordType.setName(new QName("NS", "name2"));
                    basicTypeManager.updateRecordType(recordType);
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                } 
                return incrementColumnValue;
            }
        };
        
        TypeManager typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper) {
            @Override
            protected HTableInterface getTypeTable() {
                return typeTable;
            }
        };
        basicTypeManager.createRecordType(typeManager.newRecordType(new QName("NS", "name1")));
        try {
            typeManager.createRecordType(typeManager.newRecordType(new QName("NS", "name2")));
            fail();
        } catch (TypeException expected) {
        } catch (RecordTypeExistsException expected) {
            // This will be thrown when the cache of the typeManager was updated as a consequence of the update on basicTypeManager
            // Through ZooKeeper the cache will have been marked as invalidated
        }
    }

    @Test
    public void testConcurrentFieldCreate() throws Exception {
        final HTableInterface typeTable = new LocalHTable(HBASE_PROXY.getConf(), Bytes.toBytes("typeTable")) {
            @Override
            public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                    throws IOException {
                long incrementColumnValue = super.incrementColumnValue(row, family, qualifier, amount);
                try {
                    basicTypeManager.createFieldType(basicTypeManager.newFieldType(valueType, new QName("NS", "name"), Scope.VERSIONED));
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                } 
                return incrementColumnValue;
            }
        };
        
        TypeManager typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper) {
            @Override
            protected HTableInterface getTypeTable() {
                return typeTable;
            }
        };
        try {
            typeManager.createFieldType(typeManager.newFieldType(valueType, new QName("NS", "name"), Scope.VERSIONED));
            fail();
        } catch (TypeException expected) {
        } catch (FieldTypeExistsException expected) {
            // This will be thrown when the cache of the typeManager was updated as a consequence of the update on basicTypeManager
            // Through ZooKeeper the cache will have been marked as invalidated
        }
    }
    
    @Test
    public void testConcurrentFieldUpdate() throws Exception {
        final HTableInterface typeTable = new LocalHTable(HBASE_PROXY.getConf(), Bytes.toBytes("typeTable")) {
            @Override
            public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                    throws IOException {
                long incrementColumnValue = super.incrementColumnValue(row, family, qualifier, amount);
                try {
                    FieldType fieldType = basicTypeManager.getFieldTypeByName(new QName("NS", "name1"));
                    fieldType.setName(new QName("NS", "name2"));
                    basicTypeManager.updateFieldType(fieldType);
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                } 
                return incrementColumnValue;
            }
        };
        
        TypeManager typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper) {
            @Override
            protected HTableInterface getTypeTable() {
                return typeTable;
            }
        };
        basicTypeManager.createFieldType(typeManager.newFieldType(valueType, new QName("NS", "name1"), Scope.VERSIONED));
        try {
            typeManager.createFieldType(typeManager.newFieldType(valueType, new QName("NS", "name2"), Scope.VERSIONED));
            fail();
        } catch (TypeException expected) {
        } catch (FieldTypeExistsException expected) {
            // This will be thrown when the cache of the typeManager was updated as a consequence of the update on basicTypeManager
            // Through ZooKeeper the cache will have been marked as invalidated
        }
    }
    
    @Test
    public void testGetTypeIgnoresConcurrentCounterRows() throws Exception {
        HTableInterface typeTable = new LocalHTable(HBASE_PROXY.getConf(), Bytes.toBytes("typeTable"));
        TypeManager typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper);
        UUID id = UUID.randomUUID();
        byte[] rowId;
        rowId = new byte[16];
        Bytes.putLong(rowId, 0, id.getMostSignificantBits());
        Bytes.putLong(rowId, 8, id.getLeastSignificantBits());
        typeTable.incrementColumnValue(rowId, NON_VERSIONED_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME, 1);
        try {
            typeManager.getFieldTypeById(id.toString());
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
        try {
            typeManager.getRecordTypeById(id.toString(), null);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }
}
