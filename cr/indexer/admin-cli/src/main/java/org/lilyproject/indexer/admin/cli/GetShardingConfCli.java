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
package org.lilyproject.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.IOUtils;
import org.lilyproject.indexer.model.api.IndexDefinition;

import java.io.OutputStream;
import java.util.List;

public class GetShardingConfCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-get-shardingconf";
    }

    public static void main(String[] args) {
        new GetShardingConfCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        nameOption.setRequired(true);

        options.add(nameOption);
        options.add(outputFileOption);
        options.add(forceOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        IndexDefinition index = model.getIndex(indexName);

        byte[] conf = index.getShardingConfiguration();

        if (conf == null) {
            System.out.println("The index has no sharding configuration.");
            return 1;
        }

        OutputStream os = getOutput();
        IOUtils.write(conf, os);
        os.close();

        return 0;
    }
}
