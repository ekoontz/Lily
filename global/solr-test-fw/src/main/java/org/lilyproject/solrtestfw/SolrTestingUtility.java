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
package org.lilyproject.solrtestfw;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.*;

public class SolrTestingUtility {
    private int solrPort = 6712;
    private Server server;
    private String schemaLocation;
    private TempSolrHome solrHome;

    public SolrTestingUtility(String schemaLocation) {
        this.schemaLocation = schemaLocation;
    }

    public void start() throws Exception {
        solrHome = new TempSolrHome();
        solrHome.copyDefaultConfigToSolrHome("");
        solrHome.copySchemaFromResource(schemaLocation);
        solrHome.setSystemProperties();


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

        if (solrHome != null)
            solrHome.cleanup();
    }

}
