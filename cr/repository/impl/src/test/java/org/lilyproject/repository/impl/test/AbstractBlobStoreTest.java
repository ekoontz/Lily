package org.lilyproject.repository.impl.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.util.hbase.HBaseTableUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public abstract class AbstractBlobStoreTest {
    protected static RowLog wal;
    protected static Repository repository;
    protected static TypeManager typeManager;
    protected static Configuration configuration;
    protected static ZooKeeperItf zooKeeper;
    protected static RowLogConfigurationManager rowLogConfMgr;
    
    protected static void setupWal() throws IOException, RowLogException, InterruptedException {
        rowLogConfMgr = new RowLogConfigurationManagerImpl(zooKeeper);
        wal = new RowLogImpl("WAL", HBaseTableUtil.getRecordTable(configuration), HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY, HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, true, rowLogConfMgr);
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal, 100);
        wal.registerShard(walShard);
    }
    
    @Test
    public void testCreate() throws Exception {
        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = new Blob("aMediaType", (long)bytes.length, "testCreate");
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
        Blob blob = new Blob("aMediaType", (long)bytes.length, "testCreate");
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
        Blob blob = new Blob("aMediaType", (long)10, "aName");
        try {
            repository.getInputStream(blob);
            fail();
        } catch (BlobNotFoundException expected) {
        }
    }
    
    @Test
    public void testBadEncoding() throws Exception {
        Blob blob = new Blob("aMediaType", (long)10, "aName");
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
