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

import org.apache.commons.cli.*;
import org.lilyproject.indexer.model.api.IndexDefinition;

import java.util.List;

public class AddIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-add-index";
    }

    public static void main(String[] args) {
        new AddIndexCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        nameOption.setRequired(true);
        solrShardsOption.setRequired(true);
        configurationOption.setRequired(true);

        options.add(nameOption);
        options.add(solrShardsOption);
        options.add(shardingConfigurationOption);
        options.add(configurationOption);
        options.add(generalStateOption);
        options.add(updateStateOption);
        options.add(buildStateOption);
        options.add(forceOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        IndexDefinition index = model.newIndex(indexName);

        index.setSolrShards(solrShards);

        index.setConfiguration(indexerConfiguration);

        if (shardingConfiguration != null)
            index.setShardingConfiguration(shardingConfiguration);

        if (generalState != null)
            index.setGeneralState(generalState);

        if (updateState != null)
            index.setUpdateState(updateState);

        if (buildState != null)
            index.setBatchBuildState(buildState);

        model.addIndex(index);

        System.out.println("Index created: " + indexName);

        return 0;
    }

}
