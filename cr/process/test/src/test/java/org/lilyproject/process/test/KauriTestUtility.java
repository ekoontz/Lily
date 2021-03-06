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
package org.lilyproject.process.test;

import org.apache.commons.io.FileUtils;
import org.kauriproject.runtime.KauriRuntime;
import org.kauriproject.runtime.KauriRuntimeSettings;
import org.kauriproject.runtime.configuration.ConfManager;
import org.kauriproject.runtime.configuration.ConfManagerImpl;
import org.kauriproject.runtime.model.SourceLocations;
import org.kauriproject.runtime.rapi.Mode;
import org.kauriproject.runtime.repository.ArtifactRepository;
import org.kauriproject.runtime.repository.Maven2StyleArtifactRepository;
import org.lilyproject.testfw.HBaseProxy;
import org.lilyproject.util.net.NetUtils;
import org.restlet.Client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for helping to launch Kauri in a testcase.
 *
 * <p>This is (currently?) specific to launching the Lily server process. The path to the source root
 * of the server process should be given in the constructor. In this dir, there should be a Kauri conf
 * dir called conf, and a file module-source-locations.properties containing pointers for all the
 * modules that are part of this project (= that will not necessarily already be present in the local
 * Maven repo). This is important because when running test cases that are part of a Kauri module
 * project, as the artifact of the current project will not yet be deployed in the local Maven repository.
 *
 * <p>The HTTP port for Kauri is determined dynamically and can be retrieved via {@link #getPort()}.
 */
public class KauriTestUtility {
    private KauriRuntime runtime;
    private File tmpDir;
    private File confDir;
    private int port;
    private String serverProcessSrcDir;

    /**
     *
     * @param serverProcessSrcDir relative to project base dir, should end on slash
     */
    public KauriTestUtility(String serverProcessSrcDir) {
        this.serverProcessSrcDir = "/" + serverProcessSrcDir;
        tmpDir = createTempDir();
        port = NetUtils.getFreePort();
    }

    public void start() throws Exception {
        KauriRuntimeSettings settings = new KauriRuntimeSettings();
        settings.setRepository(getRepository());
        settings.setConfManager(getConfManager());

        // We specify the module source locations, because when this test is run, the current module
        // will not yet be installed yet in the Maven repository. While we could get it from the target
        // directory, this approach makes it easier to run the tests from within your IDE.
        FileInputStream fis = new FileInputStream(new File(getBasedir() + serverProcessSrcDir + "module-source-locations.properties"));
        SourceLocations sourceLocations = new SourceLocations(fis, getBasedir() + serverProcessSrcDir);
        fis.close();
        settings.setSourceLocations(sourceLocations);

        runtime = new KauriRuntime(settings);
        runtime.setMode(Mode.getDefault());
        runtime.start();
    }

    public void stop() {
        if (runtime != null) {
            runtime.stop();
        }

        if (tmpDir != null) {
            try {
                FileUtils.deleteDirectory(tmpDir);
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public KauriRuntime getRuntime() {
        return runtime;
    }

    public Client getClient() {
        return runtime.getRestserviceManager().getComponent().getContext().getClientDispatcher();
    }

    public ConfManager getConfManager() {
        List<File> confDirs = new ArrayList<File>();
        confDirs.add(confDir);
        confDirs.add(new File(getBasedir() + serverProcessSrcDir + "conf"));
        return new ConfManagerImpl(confDirs);
    }

    public void createDefaultConf(HBaseProxy hbaseProxy) throws Exception {
        File confDir = new File(tmpDir, "conf");
        confDir.mkdir();

        String zkServer = hbaseProxy.getConf().get("hbase.zookeeper.quorum");
        String zkPort = hbaseProxy.getConf().get("hbase.zookeeper.property.clientPort");

        String blobFsUri = hbaseProxy.getBlobFS().getUri().toString();

        writeConf(confDir, "general", "zookeeper.xml",
                "<zooKeeper xmlns:conf=\"http://kauriproject.org/configuration\" conf:inherit=\"shallow\">" +
                        "<connectString>" + zkServer + ":" + zkPort + "</connectString></zooKeeper>");

        writeConf(confDir, "repository", "repository.xml",
                "<repository xmlns:conf=\"http://kauriproject.org/configuration\" conf:inherit=\"shallow\">" +
                        "<zookeeperConnectString>" + zkServer + ":" + zkPort + "</zookeeperConnectString>" +
                        "<blobFileSystem>" + blobFsUri + "/lily/blobs</blobFileSystem></repository>");

        writeConf(confDir, "general", "hbase.xml",
                "<hbase xmlns:conf=\"http://kauriproject.org/configuration\" conf:inherit=\"deep\">" +
                        "<properties conf:inheritKey='string(name)'>" +
                        "  <property>" +
                        "    <name>hbase.zookeeper.quorum</name>" +
                        "    <value>" + zkServer + "</value>" +
                        "  </property>" +
                        "  <property>" +
                        "    <name>hbase.zookeeper.property.clientPort</name>" +
                        "    <value>" + zkPort + "</value>" +
                        "  </property>" +
                        "</properties>" +
                        "</hbase>");

        writeConf(confDir, "kauri", "connectors.xml",
                "<connectors xmlns:conf=\"http://kauriproject.org/configuration\" conf:inherit=\"shallow\">" +
                        "<serverConnector protocols='HTTP' port='" + port + "'/>" +
                        "</connectors>");

        this.confDir = confDir;
    }

    public int getPort() {
        return port;
    }

    private void writeConf(File confDir, String dirName, String fileName, String content) throws IOException {
        File dir = new File(confDir, dirName);
        dir.mkdir();

        FileUtils.writeStringToFile(new File(dir, fileName), content, "UTF-8");
    }

    private File createTempDir() {
        String suffix = (System.currentTimeMillis() % 100000) + "" + (int)(Math.random() * 100000);
        File dir;
        while (true) {
            String dirName = System.getProperty("java.io.tmpdir") + File.separator + ("kauritest_") + suffix;
            dir = new File(dirName);
            if (dir.exists()) {
                System.out.println("Temporary test directory already exists, trying another location. Currenty tried: " + dirName);
                continue;
            }

            boolean dirCreated = dir.mkdirs();
            if (!dirCreated) {
                throw new RuntimeException("Failed to created temporary test directory at " + dirName);
            }

            break;
        }

        dir.mkdirs();
        dir.deleteOnExit();

        return dir;
    }

    private String getBasedir() {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            throw new RuntimeException("basedir property is not set. Are you running this test outside of Maven? If so, specify -Dbasedir=/path/to/sub_project_containing_this_test");
        }
        return basedir;
    }

    private ArtifactRepository getRepository() {
        String localRepositoryPath = System.getProperty("localRepository");
        if (localRepositoryPath == null)
            localRepositoryPath = System.getProperty("user.home") + "/.m2/repository";

        return new Maven2StyleArtifactRepository(new File(localRepositoryPath));
    }

}
