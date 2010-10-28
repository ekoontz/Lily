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
package org.lilyproject.tools.docs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.util.xml.DocumentHelper;
import org.lilyproject.util.xml.XPathUtils;
import org.outerj.daisy.repository.*;
import org.outerj.daisy.repository.Document;
import org.outerj.daisy.repository.clientimpl.RemoteRepositoryManager;
import org.outerj.daisy.repository.query.QueryManager;
import org.outerj.daisy.repository.schema.RepositorySchema;
import org.w3c.dom.*;

import java.io.InputStream;
import java.util.List;
import java.util.Locale;

/**
 * Simple tool to output all links that occur in the Lily documentation.
 */
public class LinkLister extends BaseCliTool {
    private Option collectionOption;
    private Option branchOption;

    private static final String DEFFAULT_COLLECTION = "lilydocs";
    private static final String DEFFAULT_BRANCH = "lilydocs-trunk";

    @Override
    protected String getCmdName() {
        return "lily-list-doc-links";
    }

    public static void main(String[] args) {
        new LinkLister().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        collectionOption = OptionBuilder
                .withArgName("collection")
                .hasArg()
                .withDescription("Daisy collection name, default " + DEFFAULT_COLLECTION)
                .withLongOpt("collection")
                .create("c");

        branchOption = OptionBuilder
                .withArgName("branch")
                .hasArg()
                .withDescription("Daisy branch, default " + DEFFAULT_BRANCH)
                .withLongOpt("branch")
                .create("b");

        options.add(collectionOption);
        options.add(branchOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        String collection = cmd.getOptionValue(collectionOption.getOpt());
        if (collection == null)
            collection = DEFFAULT_COLLECTION;
        String branch = cmd.getOptionValue(branchOption.getOpt());
        if (branch == null)
            branch = DEFFAULT_BRANCH;

        RepositoryManager repositoryManager = new RemoteRepositoryManager(
            "http://lilyproject.org:9263", new Credentials("guest", "guest"));
        Repository repository =
            repositoryManager.getRepository(new Credentials("guest", "guest"));
        QueryManager queryManager = repository.getQueryManager();
        RepositorySchema schema = repository.getRepositorySchema();

        String query = "select id, name where InCollection('" + collection + "') and branch = '" + branch + "'";
        VariantKey[] keys = queryManager.performQueryReturnKeys(query, Locale.getDefault());

        for (VariantKey key : keys) {
            Document doc = repository.getDocument(key, false);
            Version version = doc.getLiveVersion();
            if (version == null) {
                continue;
            }

            for (Part part : version.getParts().getArray()) {
                if (schema.getPartTypeById(part.getTypeId(), false).isDaisyHtml()) {
                    InputStream is = part.getDataStream();
                    org.w3c.dom.Document domDoc = DocumentHelper.parse(is);
                    NodeList links = XPathUtils.evalNodeList("//a/@href", domDoc.getDocumentElement());
                    for (int j = 0; j < links.getLength(); j++) {
                        String link = ((Attr)links.item(j)).getValue();
                        System.out.printf("[%1$10.10s][%2$30.30s][%3$10.10s] %4$s\n", key.getDocumentId(),
                                version.getDocumentName(), part.getTypeName(), link);
                    }
                    is.close();
                }
            }
        }

        return 0;
    }
}
