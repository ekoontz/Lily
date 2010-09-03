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
package org.lilycms.repository.impl.lock.test;


import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.impl.lock.RowLock;
import org.lilycms.repository.impl.lock.RowLocker;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

public class RowLockerTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private final static byte[] family = Bytes.toBytes("RowLockerCF");
    private final static byte[] qualifier = Bytes.toBytes("RowLockerQ");
    private final static String tableName = "RowLockerTable";
    private static HTable table;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        HBaseAdmin admin = new HBaseAdmin(HBASE_PROXY.getConf());
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor(family));
        try {
            admin.createTable(tableDescriptor);
        } catch (TableExistsException e) {
            // ok, from previous testcase run against external HBase
        }
        table = new HTable(HBASE_PROXY.getConf(), tableName);

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        HBASE_PROXY.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testLockUnlock() throws IOException {
        RowLocker locker = new RowLocker(table, family, qualifier, 600000L);
        byte[] rowKey = Bytes.toBytes("testLockUnlock");
        RowLock lock = locker.lockRow(rowKey);
        assertFalse(lock == null);
        assertTrue(locker.isLocked(rowKey));
        locker.unlockRow(lock);
        assertFalse(locker.isLocked(rowKey));
    }

    @Test
    public void testLockTwice() throws IOException {
        RowLocker locker = new RowLocker(table, family, qualifier, 600000L);
        byte[] rowKey = Bytes.toBytes("testLockTwice");
        RowLock lock = locker.lockRow(rowKey);
        assertFalse(lock == null);
        assertTrue(locker.isLocked(rowKey));
        assertNull(locker.lockRow(rowKey));
        locker.unlockRow(lock);
        assertFalse(locker.isLocked(rowKey));
    }
    
    @Test
    public void testLockTimesOut() throws Exception {
        RowLocker locker = new RowLocker(table, family, qualifier, 1L);
        byte[] rowKey = Bytes.toBytes("testLockTimesOut");
        RowLock lock = locker.lockRow(rowKey);
        assertFalse(lock == null);
        Thread.sleep(10);
        assertFalse(locker.isLocked(rowKey));
        // cleanup
        locker.unlockRow(lock);
    }

    @Test
    public void testPut() throws IOException {
        RowLocker locker = new RowLocker(table, family, qualifier, 60000L);
        byte[] rowKey = Bytes.toBytes("testPut");
        RowLock lock1 = locker.lockRow(rowKey);
        locker.unlockRow(lock1);
        RowLock lock2 = locker.lockRow(rowKey);
        Put put = new Put(rowKey);
        put.add(family, Bytes.toBytes("testQualifier"), Bytes.toBytes("testValue"));
        assertTrue(locker.put(put, lock2));
        assertFalse(locker.put(put, lock1));
        locker.unlockRow(lock2);
        assertFalse(locker.put(put, lock2));
    }
    
    @Test
    public void testPutLockOtherRow() throws IOException {
        RowLocker locker = new RowLocker(table, family, qualifier, 60000L);
        RowLock lock = locker.lockRow(Bytes.toBytes("testPutLockOtherRow"));
        Put put = new Put(Bytes.toBytes("testPutLockOtherRow2"));
        put.add(family, Bytes.toBytes("testQualifier"), Bytes.toBytes("testValue"));
        assertFalse(locker.put(put, lock));
        
        // Cleanup
        locker.unlockRow(lock);
    }

}
