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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.NullInputStream;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TempSolrHome {
    private File solrHomeDir;
    private File solrConfDir;

    public TempSolrHome() throws IOException {
        createSolrHome();
    }

    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(solrHomeDir);
    }

    public void setSystemProperties() {
        System.setProperty("solr.solr.home", solrHomeDir.getAbsolutePath());
        System.setProperty("solr.data.dir", new File(solrHomeDir, "data").getAbsolutePath());        
    }

    public File getSolrHomeDir() {
        return solrHomeDir;
    }

    private void createSolrHome() throws IOException {
        String tmpdir = System.getProperty("java.io.tmpdir");
        solrHomeDir = new File(tmpdir, "solr-" + System.currentTimeMillis());
        FileUtils.forceMkdir(solrHomeDir);

        solrConfDir = new File(solrHomeDir, "conf");
        FileUtils.forceMkdir(solrConfDir);
    }

    public void copyDefaultConfigToSolrHome(String autoCommitSetting) throws IOException {
        copyResourceFiltered("org/lilyproject/solrtestfw/conftemplate/solrconfig.xml",
                new File(solrConfDir, "solrconfig.xml"), autoCommitSetting);
        createEmptyFile(new File(solrConfDir, "synonyms.txt"));
        createEmptyFile(new File(solrConfDir, "stopwords.txt"));
        createEmptyFile(new File(solrConfDir, "protwords.txt"));
    }

    public void copySchemaFromFile(File schemaFile) throws IOException {
        FileUtils.copyFile(schemaFile, new File(solrConfDir, "schema.xml"));
    }

    public void copySchemaFromResource(String path) throws IOException {
        copyResource(path, new File(solrConfDir, "schema.xml"));
    }

    private void copyResource(String path, File destination) throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(path);
        FileUtils.copyInputStreamToFile(is, destination);
        is.close();
    }

    private void copyResourceFiltered(String path, File destination, String autoCommitSetting) throws IOException {

        InputStream is = getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        FileWriter writer = new FileWriter(destination);

        String placeholder = Pattern.quote("<!--AUTOCOMMIT_PLACEHOLDER-->");
        String replacement = Matcher.quoteReplacement(autoCommitSetting);

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.replaceAll(placeholder, replacement);
            writer.write(line);
            writer.write('\n');
        }

        reader.close();
        writer.close();
    }

    private void createEmptyFile(File destination) throws IOException {
        FileUtils.copyInputStreamToFile(new NullInputStream(0), destination);
    }
}
