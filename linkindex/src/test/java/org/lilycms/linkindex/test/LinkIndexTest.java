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
package org.lilycms.linkindex.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.hbaseindex.IndexManager;
import org.lilycms.linkindex.FieldedLink;
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.linkindex.LinkIndexUpdater;
import org.lilycms.repository.api.*;
import org.lilycms.repository.impl.*;
import org.lilycms.rowlog.api.*;
import org.lilycms.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.api.RowLogMessageListenerMapping;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.util.hbase.HBaseTableUtil;
import org.lilycms.util.io.Closer;
import org.lilycms.util.repo.VersionTag;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

public class LinkIndexTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static ZooKeeperItf zk;
    private static RowLogConfigurationManager rowLogConfMgr;

    private static TypeManager typeManager;
    private static HBaseRepository repository;
    private static IdGenerator ids;
    private static LinkIndex linkIndex;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging("org.lilycms.linkindex");

        HBASE_PROXY.start();
        zk = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);

        IndexManager.createIndexMetaTableIfNotExists(HBASE_PROXY.getConf());

        IdGenerator idGenerator = new IdGeneratorImpl();
        typeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);

        rowLogConfMgr = new RowLogConfigurationManagerImpl(zk);

        RowLog wal = new RowLogImpl("WAL", HBaseTableUtil.getRecordTable(HBASE_PROXY.getConf()),
                HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY, HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, true, rowLogConfMgr);
        RowLogShard walShard = new RowLogShardImpl("WS1", HBASE_PROXY.getConf(), wal, 100);
        wal.registerShard(walShard);

        repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, wal, HBASE_PROXY.getConf());
        ids = repository.getIdGenerator();
        IndexManager indexManager = new IndexManager(HBASE_PROXY.getConf());

        LinkIndex.createIndexes(indexManager);
        linkIndex = new LinkIndex(indexManager, repository);

        rowLogConfMgr.addSubscription("WAL", "LinkIndexUpdater", SubscriptionContext.Type.VM, 1, 1);
        RowLogMessageListenerMapping.INSTANCE.put("LinkIndexUpdater", new LinkIndexUpdater(repository, linkIndex));
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(rowLogConfMgr);
        Closer.close(zk);
        HBASE_PROXY.stop();
    }

    @Test
    public void testLinkIndex() throws Exception {
        Set<FieldedLink> links1 = new HashSet<FieldedLink>();
        links1.add(new FieldedLink(ids.newRecordId("id1"), "field1"));
        links1.add(new FieldedLink(ids.newRecordId("id2"), "field1"));

        Set<FieldedLink> links2 = new HashSet<FieldedLink>();
        links2.add(new FieldedLink(ids.newRecordId("id3"), "field1"));
        links2.add(new FieldedLink(ids.newRecordId("id4"), "field1"));

        linkIndex.updateLinks(ids.newRecordId("idA"), "live", links1);
        linkIndex.updateLinks(ids.newRecordId("idB"), "live", links1);
        linkIndex.updateLinks(ids.newRecordId("idC"), "live", links2);

        // Test forward link retrieval
        Set<FieldedLink> links = linkIndex.getForwardLinks(ids.newRecordId("idA"), "live");
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), "field1")));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), "field1")));
        assertEquals(2, links.size());

        // Test backward link retrieval
        Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("id1"), "live");
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        // Update the links for record idA and re-check
        links1.add(new FieldedLink(ids.newRecordId("id2a"), "field1"));
        linkIndex.updateLinks(ids.newRecordId("idA"), "live", links1);

        links = linkIndex.getForwardLinks(ids.newRecordId("idA"), "live");
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id1"), "field1")));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2"), "field1")));
        assertTrue(links.contains(new FieldedLink(ids.newRecordId("id2a"), "field1")));
        assertEquals(3, links.size());

        referrers = linkIndex.getReferrers(ids.newRecordId("id1"), "live");
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertTrue(referrers.contains(ids.newRecordId("idB")));
        assertEquals(2, referrers.size());

        referrers = linkIndex.getReferrers(ids.newRecordId("id2a"), "live");
        assertTrue(referrers.contains(ids.newRecordId("idA")));
        assertEquals(1, referrers.size());
    }

    @Test
    public void testLinkIndexUpdater() throws Exception {
        FieldType nonVersionedFt = typeManager.newFieldType(typeManager.getValueType("LINK", false, false),
                new QName("ns", "link1"), Scope.NON_VERSIONED);
        nonVersionedFt = typeManager.createFieldType(nonVersionedFt);

        FieldType versionedFt = typeManager.newFieldType(typeManager.getValueType("LINK", true, false),
                new QName("ns", "link2"), Scope.VERSIONED);
        versionedFt = typeManager.createFieldType(versionedFt);

        FieldType versionedMutableFt = typeManager.newFieldType(typeManager.getValueType("LINK", true, false),
                new QName("ns", "link3"), Scope.VERSIONED_MUTABLE);
        versionedMutableFt = typeManager.createFieldType(versionedMutableFt);

        RecordType recordType = typeManager.newRecordType(new QName("ns", "MyRecordType"));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(nonVersionedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(versionedFt.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(versionedMutableFt.getId(), false));
        recordType = typeManager.createRecordType(recordType);

        //
        // Link extraction from a record without versions
        //
        {
            Record record = repository.newRecord();
            record.setRecordType(recordType.getName());
            record.setField(nonVersionedFt.getName(), new Link(ids.newRecordId("foo1")));
            record = repository.create(record);

            Set<RecordId> referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), VersionTag.VERSIONLESS_TAG);
            assertEquals(1, referrers.size());
            assertTrue(referrers.contains(record.getId()));

            referrers = linkIndex.getReferrers(ids.newRecordId("bar1"), VersionTag.VERSIONLESS_TAG);
            assertEquals(0, referrers.size());

            // Now perform an update so that there is a version
            record.setField(versionedFt.getName(), Arrays.asList(new Link(ids.newRecordId("foo2")), new Link(ids.newRecordId("foo3"))));
            record = repository.update(record);

//            recordEvent = new RecordEvent();
//            recordEvent.setVersionCreated(record.getVersion());
//            recordEvent.addUpdatedField(versionedFt.getId());
//            message = new TestQueueMessage(EventType.EVENT_RECORD_UPDATED, record.getId(), recordEvent.toJsonBytes());
//            queue.broadCastMessage(message);
//
//            // Since there is a version but no vtag yet, there should be nothing in the link index for this record
//            referrers = linkIndex.getReferrers(ids.newRecordId("foo1"), VersionTag.VERSIONLESS_TAG);
//            assertEquals(0, referrers.size());
//
//            assertEquals(0, linkIndex.getAllForwardLinks(record.getId()).size());

        }
    }
}
