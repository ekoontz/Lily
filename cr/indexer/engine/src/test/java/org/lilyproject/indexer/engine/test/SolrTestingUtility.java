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
package org.lilyproject.indexer.engine.test;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.*;

public class SolrTestingUtility {
    private int solrPort = 6712;
    private Server server;
    private File solrHomeDir;
    private String schemaLocation;

    public SolrTestingUtility(String schemaLocation) {
        this.schemaLocation = schemaLocation;
    }

    public void start() throws Exception {
        // Create SOLR home dir
        solrHomeDir = createSolrHome();
        System.setProperty("solr.solr.home", solrHomeDir.getAbsolutePath());
        System.setProperty("solr.data.dir", new File(solrHomeDir, "data").getAbsolutePath());
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Created temporary SOLR home directory at:");
        System.out.println(solrHomeDir.getAbsolutePath());
        System.out.println("------------------------------------------------------------------------");

        // Launch SOLR
        String solrWar = System.getProperty("solr.war");
        if (solrWar == null || !new File(solrWar).exists()) {
            System.out.println();
            System.out.println("------------------------------------------------------------------------");
            System.out.println("SOLR not found at");
            System.out.println(solrWar);
            System.out.println("Verify setting of solr.war system property");
            System.out.println("------------------------------------------------------------------------");
            System.out.println();
            throw new Exception("SOLR war not found at " + solrWar);
        }

        server = new Server(solrPort);
        server.addHandler(new WebAppContext(solrWar, "/"));

        server.start();
    }

    public String getUri() {
        return "http://localhost:" + solrPort;
    }

    public void stop() throws Exception {
        if (server != null)
            server.stop();

        delete(solrHomeDir);
    }

    private void delete(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                delete(child);
            }
        }
        file.delete();
    }

    private File createSolrHome() throws IOException {
        String tmpdir = System.getProperty("java.io.tmpdir");
        File solrHomeDir = new File(tmpdir, "solr-" + System.currentTimeMillis());
        solrHomeDir.mkdirs();

        File solrConfDir = new File(solrHomeDir, "conf");
        solrConfDir.mkdir();

        // Ignoring the closing of the streams here, it's just a testcase anyway ...

        copyStream(getClass().getClassLoader().getResourceAsStream(schemaLocation),
                new File(solrConfDir, "schema.xml"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilyproject/indexer/engine/test/solrconfig.xml"),
                new File(solrConfDir, "solrconfig.xml"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilyproject/indexer/engine/test/synonyms.txt"),
                new File(solrConfDir, "synonyms.txt"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilyproject/indexer/engine/test/synonyms.txt"),
                new File(solrConfDir, "stopwords.txt"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilyproject/indexer/engine/test/synonyms.txt"),
                new File(solrConfDir, "protwords.txt"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilyproject/indexer/engine/test/elevate.xml"),
                new File(solrConfDir, "elevate.xml"));

        return solrHomeDir;
    }

    private void copyStream(InputStream is, File target) throws IOException {
        FileOutputStream fos = new FileOutputStream(target);
        byte[] buffer = new byte[8192];
        int length;
        while ((length = is.read(buffer, 0, buffer.length)) != -1) {
            fos.write(buffer, 0, length);
        }
        fos.close();
    }
}
