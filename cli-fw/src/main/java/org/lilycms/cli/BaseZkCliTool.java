package org.lilycms.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import java.util.List;

/**
 * Base for CLI tools that need a ZooKeeper connect string.
 */
public abstract class BaseZkCliTool extends BaseCliTool {
    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";

    protected String zkConnectionString;

    protected Option zkOption;

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        zkOption = OptionBuilder
                .withArgName("connection-string")
                .hasArg()
                .withDescription("ZooKeeper connection string: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");

        options.add(zkOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0)
            return result;

        if (!cmd.hasOption(zkOption.getOpt())) {
            System.out.println("ZooKeeper connection string not specified, using default: " + DEFAULT_ZK_CONNECT);
            System.out.println();
            zkConnectionString = DEFAULT_ZK_CONNECT;
        } else {
            zkConnectionString = cmd.getOptionValue(zkOption.getOpt());
        }

        return 0;
    }
}
