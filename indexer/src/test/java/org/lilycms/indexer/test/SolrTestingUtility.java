package org.lilycms.indexer.test;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.*;

public class SolrTestingUtility {
    private int solrPort = 6712;
    private SolrServer solrServer;
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
            System.out.println("Verify setting of <solr.war> property in settings.xml");
            System.out.println("------------------------------------------------------------------------");
            System.out.println();
            throw new Exception("SOLR war not found at " + solrWar);
        }

        server = new Server(solrPort);
        server.addHandler(new WebAppContext(solrWar, "/"));

        server.start();

        solrServer = new CommonsHttpSolrServer("http://localhost:" + solrPort);
    }

    public SolrServer getSolrServer() {
        return solrServer;
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

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/solrconfig.xml"),
                new File(solrConfDir, "solrconfig.xml"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/synonyms.txt"),
                new File(solrConfDir, "synonyms.txt"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/synonyms.txt"),
                new File(solrConfDir, "stopwords.txt"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/synonyms.txt"),
                new File(solrConfDir, "protwords.txt"));

        copyStream(getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/elevate.xml"),
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
