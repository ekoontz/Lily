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
package org.lilycms.repository.api.tutorial;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.*;
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
import org.lilycms.util.io.Closer;
import org.lilycms.util.repo.PrintUtil;
import org.lilycms.util.zookeeper.StateWatchingZooKeeper;
import org.lilycms.util.zookeeper.ZooKeeperItf;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

/**
 * The code in this class is used in the repository API tutorial (390-OTC). If this
 * code needs updating because of API changes, then the tutorial itself probably needs
 * to be updated too.
 */
public class TutorialTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();

    private static final String NS = "org.lilycms.tutorial";

    private static TypeManager typeManager;
    private static HBaseRepository repository;
    private static RowLog wal;
    private static Configuration configuration;
    private static ZooKeeperItf zooKeeper;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();

        IdGenerator idGenerator = new IdGeneratorImpl();
        configuration = HBASE_PROXY.getConf();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);

        typeManager = new HBaseTypeManager(idGenerator, configuration);

        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        SizeBasedBlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        blobStoreAccessFactory.addBlobStoreAccess(Long.MAX_VALUE, dfsBlobStoreAccess);
        setupWal();
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreAccessFactory, wal, configuration);

    }
    
    protected static void setupWal() throws IOException, RowLogException {
        wal = new RowLogImpl("WAL", HBaseTableUtil.getRecordTable(configuration), HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY, HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, true, zooKeeper);
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal, 100);
        wal.registerShard(walShard);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }

    @Test
    public void createRecordType() throws Exception {
        // (1)
        ValueType stringValueType = typeManager.getValueType("STRING", false, false);

        // (2)
        FieldType title = typeManager.newFieldType(stringValueType, new QName(NS, "title"), Scope.VERSIONED);

        // (3)
        title = typeManager.createFieldType(title);

        // (4)
        RecordType book = typeManager.newRecordType(new QName(NS, "Book"));
        book.addFieldTypeEntry(title.getId(), true);

        // (5)
        book = typeManager.createRecordType(book);

        // (6)
        PrintUtil.print(book, repository);
    }

    @Test
    public void updateRecordType() throws Exception {
        ValueType stringValueType = typeManager.getValueType("STRING", false, false);
        ValueType stringMvValueType = typeManager.getValueType("STRING", true, false);
        ValueType longValueType = typeManager.getValueType("LONG", false, false);
        ValueType dateValueType = typeManager.getValueType("DATE", false, false);
        ValueType blobValueType = typeManager.getValueType("BLOB", false, false);
        ValueType linkValueType = typeManager.getValueType("LINK", false, false);

        FieldType description = typeManager.newFieldType(blobValueType, new QName(NS, "description"), Scope.VERSIONED);
        description = typeManager.createFieldType(description);

        FieldType authors = typeManager.newFieldType(stringMvValueType, new QName(NS, "authors"), Scope.VERSIONED);
        authors = typeManager.createFieldType(authors);

        FieldType released = typeManager.newFieldType(dateValueType, new QName(NS, "released"), Scope.VERSIONED);
        released = typeManager.createFieldType(released);

        FieldType pages = typeManager.newFieldType(longValueType, new QName(NS, "pages"), Scope.VERSIONED);
        pages = typeManager.createFieldType(pages);

        FieldType sequelTo = typeManager.newFieldType(linkValueType, new QName(NS, "sequel_to"), Scope.VERSIONED);
        sequelTo = typeManager.createFieldType(sequelTo);

        FieldType manager = typeManager.newFieldType(stringValueType, new QName(NS, "manager"), Scope.NON_VERSIONED);
        manager = typeManager.createFieldType(manager);

        FieldType reviewStatus = typeManager.newFieldType(stringValueType, new QName(NS, "review_status"), Scope.VERSIONED_MUTABLE);
        reviewStatus = typeManager.createFieldType(reviewStatus);

        RecordType book = typeManager.getRecordTypeByName(new QName(NS, "Book"), null);

        // The order in which fields are added does not matter
        book.addFieldTypeEntry(description.getId(), false);
        book.addFieldTypeEntry(authors.getId(), false);
        book.addFieldTypeEntry(released.getId(), false);
        book.addFieldTypeEntry(pages.getId(), false);
        book.addFieldTypeEntry(sequelTo.getId(), false);
        book.addFieldTypeEntry(manager.getId(), false);
        book.addFieldTypeEntry(reviewStatus.getId(), false);

        // Now we call updateRecordType instead of createRecordType
        book = typeManager.updateRecordType(book);

        PrintUtil.print(book, repository);
    }

    @Test
    public void createRecord() throws Exception {
        // (1)
        Record record = repository.newRecord();

        // (2)
        record.setRecordType(new QName(NS, "Book"));

        // (3)
        record.setField(new QName(NS, "title"), "Lily, the definitive guide, 3rd edition");

        // (4)
        record = repository.create(record);

        // (5)
        PrintUtil.print(record, repository);
    }

    @Test
    public void createRecordUserSpecifiedId() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.newRecord(id);
        record.setRecordType(new QName(NS, "Book"));
        record.setField(new QName(NS, "title"), "Lily, the definitive guide, 3rd edition");
        record = repository.create(record);

        PrintUtil.print(record, repository);
    }

    @Test
    public void updateRecord() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.newRecord(id);
        record.setField(new QName(NS, "title"), "Lily, the definitive guide, third edition");
        record.setField(new QName(NS, "pages"), Long.valueOf(912));
        record.setField(new QName(NS, "manager"), "Manager M");
        record = repository.update(record);

        PrintUtil.print(record, repository);
    }

    @Test
    public void updateRecordViaRead() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.read(id);
        record.setField(new QName(NS, "released"), new LocalDate());
        record.setField(new QName(NS, "authors"), Arrays.asList("Author A", "Author B"));
        record.setField(new QName(NS, "review_status"), "reviewed");
        record = repository.update(record);

        PrintUtil.print(record, repository);
    }

    @Test
    public void readRecord() throws Exception {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");


        // (1)
        Record record = repository.read(id);
        String title = (String)record.getField(new QName(NS, "title")); 
        System.out.println(title);

        // (2)
        record = repository.read(id, 1L);
        System.out.println(record.getField(new QName(NS, "title")));

        // (3)
        record = repository.read(id, 1L, Arrays.asList(new QName(NS, "title")));
        System.out.println(record.getField(new QName(NS, "title")));
    }

    @Test
    public void blob() throws Exception {
        //
        // Write a blob
        //

        String description = "<html><body>This book gives thorough insight into Lily, ...</body></html>";
        byte[] descriptionData = description.getBytes("UTF-8");

        // (1)
        Blob blob = new Blob("text/html", (long)descriptionData.length, "description.xml");
        OutputStream os = repository.getOutputStream(blob);
        try {
            os.write(descriptionData);
        } finally {
            os.close();
        }

        // (2)
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = repository.newRecord(id);
        record.setField(new QName(NS, "description"), blob);
        record = repository.update(record);

        //
        // Read a blob
        //
        InputStream is = null;
        try {
            is = repository.getInputStream((Blob)record.getField(new QName(NS, "description")));
            System.out.println("Data read from blob is:");
            Reader reader = new InputStreamReader(is, "UTF-8");
            char[] buffer = new char[20];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                System.out.print(new String(buffer, 0, read));
            }
            System.out.println();
        } finally {
            if (is != null) is.close();
        }        
    }

    @Test
    public void variantRecord() throws Exception {
        // (1)
        IdGenerator idGenerator = repository.getIdGenerator();
        RecordId masterId = idGenerator.newRecordId();

        // (2)
        Map<String, String> variantProps = new HashMap<String, String>();
        variantProps.put("language", "en");

        // (3)
        RecordId enId = idGenerator.newRecordId(masterId, variantProps);

        // (4)
        Record enRecord = repository.newRecord(enId);
        enRecord.setRecordType(new QName(NS, "Book"));
        enRecord.setField(new QName(NS, "title"), "Car maintenance");
        enRecord = repository.create(enRecord);

        // (5)
        RecordId nlId = idGenerator.newRecordId(enRecord.getId().getMaster(), Collections.singletonMap("language", "nl"));
        Record nlRecord = repository.newRecord(nlId);
        nlRecord.setRecordType(new QName(NS, "Book"));
        nlRecord.setField(new QName(NS, "title"), "Wagen onderhoud");
        nlRecord = repository.create(nlRecord);

        // (6)
        Set<RecordId> variants = repository.getVariants(masterId);
        for (RecordId variant : variants) {
            System.out.println(variant);
        }
    }

    @Test
    public void linkField() throws Exception {
        // (1)
        Record record1 = repository.newRecord();
        record1.setRecordType(new QName(NS, "Book"));
        record1.setField(new QName(NS, "title"), "Fishing 1");
        record1 = repository.create(record1);

        // (2)
        Record record2 = repository.newRecord();
        record2.setRecordType(new QName(NS, "Book"));
        record2.setField(new QName(NS, "title"), "Fishing 2");
        record2.setField(new QName(NS, "sequel_to"), new Link(record1.getId()));
        record2 = repository.create(record2);

        // (3)
        Link sequelToLink = (Link)record2.getField(new QName(NS, "sequel_to"));
        RecordId sequelTo = sequelToLink.resolve(record2.getId(), repository.getIdGenerator());
        Record linkedRecord = repository.read(sequelTo);
        System.out.println(linkedRecord.getField(new QName(NS, "title")));
    }

}
