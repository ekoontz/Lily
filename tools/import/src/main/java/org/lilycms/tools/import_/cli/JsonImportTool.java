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
package org.lilycms.tools.import_.cli;

import org.apache.commons.cli.*;
import org.lilycms.cli.BaseZkCliTool;
import org.lilycms.client.LilyClient;
import org.lilycms.util.io.Closer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class JsonImportTool extends BaseZkCliTool {
    private Option schemaOnlyOption;

    @Override
    protected String getCmdName() {
        return "lily-import";
    }

    public static void main(String[] args) throws Exception {
        new JsonImportTool().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        schemaOnlyOption = OptionBuilder
                .withDescription("Only import the field types and record types, not the records.")
                .withLongOpt("schema-only")
                .create("s");
        options.add(schemaOnlyOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        if (cmd.getArgList().size() < 1) {
            System.out.println("No import file specified!");
            return 1;
        }

        boolean schemaOnly = cmd.hasOption(schemaOnlyOption.getOpt());

        LilyClient client = new LilyClient(zkConnectionString, 10000);

        JsonImport jsonImport = new JsonImport(client.getRepository(), new DefaultImportListener());
        
        for (String arg : (List<String>)cmd.getArgList()) {
            System.out.println("----------------------------------------------------------------------");
            System.out.println("Importing " + arg);
            InputStream is = new FileInputStream(arg);
            try {
                jsonImport.load(client.getRepository(), is, schemaOnly);
            } finally {
                Closer.close(is);
            }
        }

        return 0;
    }
}
