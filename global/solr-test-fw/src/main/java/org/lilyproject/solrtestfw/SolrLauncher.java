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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.util.xml.DocumentHelper;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;
import org.w3c.dom.Document;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SolrLauncher extends BaseCliTool {
    private static final String SOLR_WAR_PROP = "lily.solrlauncher.war";
    private static final int DEFAULT_PORT = 8983;

    private Option schemaOption;
    private Option portOption;
    private Option commitOption;

    private Log log = LogFactory.getLog(getClass());

    private Server server;

    @Override
    protected String getCmdName() {
        return "launch-solr";
    }

    public static void main(String[] args) {
        new SolrLauncher().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        schemaOption = OptionBuilder
                .withArgName("schema.xml")
                .hasArg()
                .withDescription("SOLR schema file name")
                .withLongOpt("schema")
                .create("s");
        options.add(schemaOption);

        commitOption = OptionBuilder
                .withArgName("seconds")
                .hasArg()
                .withDescription("Auto commit index within this amount of seconds (default: no auto commit)")
                .withLongOpt("commit")
                .create("c");
        options.add(commitOption);

        portOption = OptionBuilder
                .withArgName("port-number")
                .hasArg()
                .withDescription("HTTP port number to listen on (default: " + DEFAULT_PORT + ")")
                .withLongOpt("port")
                .create("p");
        options.add(portOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        String solrWar = System.getProperty(SOLR_WAR_PROP);
        if (solrWar == null) {
            System.err.println("System property that points to SOLR war is not set: " + SOLR_WAR_PROP);
            return 1;
        }

        File solrWarFile = new File(solrWar);
        if (!solrWarFile.exists()) {
            System.err.println("SOLR war refered to by system property " + SOLR_WAR_PROP + " does not exist:");
            System.err.println(solrWarFile.getAbsolutePath());
            return 1;
        }
        solrWar = solrWarFile.getAbsolutePath();

        String schema = cmd.getOptionValue(schemaOption.getOpt());
        if (schema == null) {
            System.err.println("No schema file specified, get help via -" + helpOption.getOpt());
            return 1;
        }

        result = checkSolrSchema(schema);
        if (result != 0)
            return result;

        String autoCommitSetting = "";
        int autoCommitTime = -1;
        if (cmd.hasOption(commitOption.getOpt())) {
            try {
                autoCommitTime = Integer.parseInt(cmd.getOptionValue(commitOption.getOpt()));
                autoCommitSetting = "<autoCommit><maxTime>" + (autoCommitTime * 1000) + "</maxTime></autoCommit>";
            } catch (NumberFormatException e) {
                System.err.println("commit option should specify an integer, not: " + cmd.getOptionValue(commitOption.getOpt()));
                return 1;
            }
        }

        int port = DEFAULT_PORT;
        if (cmd.hasOption(portOption.getOpt())) {
            try {
                port = Integer.parseInt(cmd.getOptionValue(portOption.getOpt()));
            } catch (NumberFormatException e) {
                System.err.println("port option should specify an integer, not: " + cmd.getOptionValue(portOption.getOpt()));
                return 1;
            }
        }

        final TempSolrHome solrHome = new TempSolrHome();
        solrHome.copyDefaultConfigToSolrHome(autoCommitSetting);
        solrHome.copySchemaFromFile(new File(schema));
        solrHome.setSystemProperties();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (server != null) {
                        try {
                            server.stop();
                        } catch (Exception e) {
                            log.info("Error stopping Jetty", e);
                        }
                    }

                    System.out.println("Removing temporary directory " + solrHome.getSolrHomeDir());
                    solrHome.cleanup();
                } catch (IOException e) {
                    log.info("Error cleaning temporary SOLR directory", e);
                }
            }
        });

        server = new Server(port);
        server.addHandler(new WebAppContext(solrWar, "/solr"));

        server.start();

        System.out.println("-----------------------------------------------");
        System.out.println("SOLR started.");
        System.out.println();
        System.out.println("Use this as SOLR URL when creating an index:");
        System.out.println("http://localhost:" + port + "/solr");
        System.out.println();
        System.out.println("Web GUI available at:");
        System.out.println("http://localhost:" + port + "/solr/admin/");
        System.out.println();
        if (autoCommitTime == -1) {
            System.out.println("Index is not auto-committed, you can commit it using:");
            System.out.println("curl http://localhost:" + port + "/solr/update -H 'Content-type:text/xml' --data-binary '<commit/>'");
        } else {
            System.out.println("Index auto commit: " + autoCommitTime + " seconds");
        }
        System.out.println("-----------------------------------------------");

        return 0;
    }

    private int checkSolrSchema(String schema) {
        File schemaFile = new File(schema);
        if (!schemaFile.exists()) {
            System.err.println("Specified SOLR schema file does not exist:");
            System.err.println(schemaFile.getAbsolutePath());
            return 1;
        }

        Document document;
        try {
            document = DocumentHelper.parse(schemaFile);
        } catch (Exception e) {
            System.err.println("Error reading or parsing SOLR schema file.");
            System.err.println();
            e.printStackTrace();
            return 1;
        }

        if (!document.getDocumentElement().getLocalName().equals("schema")) {
            System.err.println("A SOLR schema file should have a <schema> root element, which the following file");
            System.err.println("has not:");
            System.err.println(schemaFile.getAbsolutePath());
            return 1;
        }

        return 0;
    }
}