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


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.StateWatchingZooKeeper;

public class TypeManagerFieldTypeTest extends AbstractTypeManagerFieldTypeTest {

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private static StateWatchingZooKeeper zooKeeper;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        zooKeeper = new StateWatchingZooKeeper(HBASE_PROXY.getZkConnectString(), 10000);
        typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper);
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
    }

}
