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


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.lilyproject.repository.api.BlobStoreAccessFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.avro.AvroConverter;
import org.lilyproject.repository.avro.AvroLily;
import org.lilyproject.repository.avro.AvroLilyImpl;
import org.lilyproject.repository.avro.LilySpecificResponder;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.repository.impl.RemoteRepository;
import org.lilyproject.repository.impl.RemoteTypeManager;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.rowlog.impl.RowLogImpl;
import org.lilyproject.rowlog.impl.RowLogShardImpl;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.testfw.TestHelper;
import org.lilyproject.util.hbase.HBaseTableUtil;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

/**
 *
 */
public class AvroTypeManagerRecordTypeTest extends AbstractTypeManagerRecordTypeTest {

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();

    private static Repository repository;

    private static RowLog wal;

    private static Configuration configuration;

    private static ZooKeeperItf zooKeeper;

    private static Server lilyServer;

    private static RowLogConfigurationManager rowLogConfMgr;

    private static TypeManager serverTypeManager;

    private static Repository serverRepository;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
        configuration = HBASE_PROXY.getConf();
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);
        serverTypeManager = new HBaseTypeManager(idGenerator, configuration, zooKeeper);
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS(), new Path("/lily/blobs"));
        BlobStoreAccessFactory blobStoreAccessFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        setupWal();
        serverRepository = new HBaseRepository(serverTypeManager, idGenerator, blobStoreAccessFactory, wal, configuration);
        
        AvroConverter serverConverter = new AvroConverter();
        serverConverter.setRepository(serverRepository);
        lilyServer = new HttpServer(
                new LilySpecificResponder(AvroLily.class, new AvroLilyImpl(serverRepository, serverConverter),
                        serverConverter), 0);
        lilyServer.start();
        AvroConverter remoteConverter = new AvroConverter();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);
        typeManager = new RemoteTypeManager(new InetSocketAddress(lilyServer.getPort()),
                remoteConverter, idGenerator, zooKeeper);
        Repository repository = new RemoteRepository(new InetSocketAddress(lilyServer.getPort()),
                remoteConverter, (RemoteTypeManager)typeManager, idGenerator, blobStoreAccessFactory);
        remoteConverter.setRepository(repository);
        ((RemoteTypeManager)typeManager).start();
        setupFieldTypes();
    }
    
    protected static void setupWal() throws IOException, RowLogException, InterruptedException {
        rowLogConfMgr = new RowLogConfigurationManagerImpl(zooKeeper);
        wal = new RowLogImpl("WAL", HBaseTableUtil.getRecordTable(configuration), RecordCf.WAL_PAYLOAD.bytes, RecordCf.WAL_STATE.bytes, 10000L, true, rowLogConfMgr);
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal, 100);
        wal.registerShard(walShard);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(typeManager);
        Closer.close(repository);
        Closer.close(rowLogConfMgr);
        lilyServer.close();
        lilyServer.join();
        Closer.close(serverTypeManager);
        Closer.close(serverRepository);
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }
 

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
}
