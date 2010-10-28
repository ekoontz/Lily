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
import org.lilyproject.indexer.model.api.IndexDefinition;

import java.util.List;

public class TouchIndexCli extends BaseIndexerAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-touch-index";
    }

    public static void main(String[] args) {
        new TouchIndexCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        nameOption.setRequired(true);

        options.add(nameOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        if (!model.hasIndex(indexName)) {
            System.out.println("Index does not exist: " + indexName);
            return 1;
        }

        String lock = model.lockIndex(indexName);
        try {
            IndexDefinition index = model.getMutableIndex(indexName);
            model.updateIndex(index, lock);

            System.out.println("Index touched.");
        } finally {
            model.unlockIndex(lock);
        }

        return 0;
    }
}

