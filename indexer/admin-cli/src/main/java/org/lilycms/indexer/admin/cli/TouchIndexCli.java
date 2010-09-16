package org.lilycms.indexer.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilycms.indexer.model.api.IndexDefinition;

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

