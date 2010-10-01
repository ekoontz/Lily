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

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.HierarchyPath;
import org.lilycms.repository.api.Link;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.impl.AbstractTypeManager;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.util.hbase.HBaseTableUtil;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.StateWatchingZooKeeper;
import org.lilycms.util.zookeeper.ZooKeeperItf;

public class ValueTypeTest {

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static ZooKeeperItf zooKeeper;

    private AbstractTypeManager typeManager;
    private HBaseRepository repository;

    private IdGeneratorImpl idGenerator;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }

    private RowLog initializeWal(Configuration configuration) throws IOException, RowLogException {
        RowLog wal = new RowLogImpl("WAL", HBaseTableUtil.getRecordTable(configuration), HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY, HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, true, zooKeeper);
        // Work with only one shard for now
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal, 100);
        wal.registerShard(walShard);
        return wal;
    }
    
    @Before
    public void setUp() throws Exception {
        idGenerator = new IdGeneratorImpl();
        typeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        BlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, initializeWal(HBASE_PROXY.getConf()), HBASE_PROXY.getConf());
    }

    @After
    public void tearDown() throws Exception {
        repository.stop();
    }
    
    @Test
    public void testStringType() throws Exception {
        runValueTypeTests("stringRecordTypeId", "STRING", "foo", "bar", "pub");
    }

    @Test
    public void testIntegerType() throws Exception {
        runValueTypeTests("integerRecordTypeId", "INTEGER", Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
    }

    @Test
    public void testLongType() throws Exception {
        runValueTypeTests("longRecordTypeId", "LONG", Long.MIN_VALUE, Long.valueOf(0), Long.MAX_VALUE);
    }
    
    @Test
    public void testDoubleType() throws Exception {
        runValueTypeTests("doubleRecordTypeId", "DOUBLE", Double.MIN_VALUE, Double.valueOf(0), Double.MAX_VALUE);
    }
    
    @Test
    public void testDecimalType() throws Exception {
        runValueTypeTests("decimalRecordTypeId", "DECIMAL", BigDecimal.valueOf(Double.MIN_EXPONENT), BigDecimal.ZERO, BigDecimal.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void testBooleanType() throws Exception {
        runValueTypeTests("booleanRecordTypeId", "BOOLEAN", true, false, true);
    }

    @Test
    public void testDateTimeType() throws Exception {
        runValueTypeTests("dateTimeRecordTypeId", "DATETIME", new DateTime(), new DateTime(Long.MAX_VALUE), new DateTime(Long.MIN_VALUE));
    }

    @Test
    public void testDateType() throws Exception {
        runValueTypeTests("dateRecordTypeId", "DATE", new LocalDate(), new LocalDate(2900, 10, 14), new LocalDate(1300, 5, 4));
    }

    @Test
    public void testLinkType() throws Exception {
        runValueTypeTests("linkRecordTypeId", "LINK", new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId()));
    }

    @Test
    public void testUriType() throws Exception {
        runValueTypeTests("uriRecordTypeId", "URI", URI.create("http://foo.com/bar"), URI.create("file://foo/com/bar.txt"), URI.create("https://site/index.html"));
    }
    
    @Test
    public void testBlobType() throws Exception {
        Blob blob1 = new Blob(Bytes.toBytes("aKey"), "text/html", Long.MAX_VALUE, null);
        Blob blob2 = new Blob(Bytes.toBytes("anotherKey"), "image/jpeg", Long.MIN_VALUE, "images/image.jpg");
        Blob blob3 = new Blob("text/plain", Long.valueOf(0), null);
        runValueTypeTests("blobTypeId", "BLOB", blob1, blob2, blob3);
    }

    @Test
    public void testNewPrimitiveType() throws Exception {
        typeManager.registerPrimitiveValueType(new XYPrimitiveValueType());
        runValueTypeTests("xyRecordTypeId", "XY", new XYCoordinates(-1, 1), new XYCoordinates(Integer.MIN_VALUE, Integer.MAX_VALUE), new XYCoordinates(666, 777));
    }

    private void runValueTypeTests(String name, String primitiveValueType, Object value1, Object value2, Object value3) throws Exception {
        testType(name, primitiveValueType, false, false, value1);
        testType(name, primitiveValueType, true, false, Arrays.asList(new Object[] { value1,
                        value2 }));
        testType(name, primitiveValueType, false, true, new HierarchyPath(new Object[] { value1,
                        value2 }));
        testType(name, primitiveValueType, true, true, Arrays.asList(new HierarchyPath[] {
                new HierarchyPath(new Object[] { value1, value2 }),
                new HierarchyPath(new Object[] { value1, value3 }) }));
    }
    
    private void testType(String name, String valueTypeString, boolean multivalue, boolean hierarchical,
                    Object fieldValue) throws Exception {
        QName fieldTypeName = new QName(null, valueTypeString+"FieldId"+multivalue+hierarchical);
        FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType(
                        valueTypeString, multivalue, hierarchical), fieldTypeName, Scope.VERSIONED));
        RecordType recordType = typeManager.newRecordType(new QName(null, name+"RecordTypeId"+multivalue+hierarchical));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType.getId(), true));
        recordType = typeManager.createRecordType(recordType);

        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType.getName(), recordType.getVersion());
        record.setField(fieldType.getName(), fieldValue);
        repository.create(record);

        Record actualRecord = repository.read(record.getId());
        assertEquals(fieldValue, actualRecord.getField(fieldType.getName()));
    }

    private class XYPrimitiveValueType implements PrimitiveValueType {

        private final String NAME = "XY";

        public String getName() {
            return NAME;
        }

        public Object fromBytes(byte[] bytes) {
            int x = Bytes.toInt(bytes, 0, Bytes.SIZEOF_INT);
            int y = Bytes.toInt(bytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            return new XYCoordinates(x, y);
        }

        public byte[] toBytes(Object value) {
            byte[] result = new byte[0];
            result = Bytes.add(result, Bytes.toBytes(((XYCoordinates) value).getX()));
            result = Bytes.add(result, Bytes.toBytes(((XYCoordinates) value).getY()));
            return result;
        }

        public Class getType() {
            return XYCoordinates.class;
        }
    }

    private class XYCoordinates {
        private final int x;
        private final int y;

        public XYCoordinates(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + x;
            result = prime * result + y;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            XYCoordinates other = (XYCoordinates) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (x != other.x)
                return false;
            if (y != other.y)
                return false;
            return true;
        }

        private ValueTypeTest getOuterType() {
            return ValueTypeTest.this;
        }
    }
}
