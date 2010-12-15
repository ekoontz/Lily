package org.lilyproject.tools.printhost;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseCliTool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

public class PrintHostTool extends BaseCliTool {
    protected Option nameserverOption;

    @Override
    protected String getCmdName() {
        return "lily-print-host";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        nameserverOption = OptionBuilder
                .withArgName("nameserver")
                .hasArg()
                .withDescription("Non-default nameserver to use.")
                .withLongOpt("host")
                .create("n");

        options.add(nameserverOption);

        return options;
    }

    public static void main(String[] args) {
        new PrintHostTool().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        System.out.println("Below we print the detected host name and address.");
        System.out.println("These are used by Lily and Hadoop. For example, this is what is");
        System.out.println("published in ZooKeeper so that other nodes or clients can");
        System.out.println("connect to this node.");
        System.out.println();
        System.out.println("If this shows localhost and 127.0.0.1, adjust your system setup.");
        System.out.println();

        String nameserver = "default";
        if (cmd.hasOption(nameserverOption.getOpt())) {
            nameserver = cmd.getOptionValue(nameserverOption.getOpt());
        }

        System.out.println("Using nameserver: " + nameserver);

        String cn = InetAddress.getLocalHost().getCanonicalHostName();
        System.out.println("Canonical host name: " + cn);

        InetSocketAddress ad = new InetSocketAddress(cn, 1234);
        System.out.println("Address of the canonical host name: " + ad.getAddress().getHostAddress());

        System.out.println();
        System.out.println("If you use the hostname \"" + cn + "\" in your ZooKeeper connection");
        System.out.println("string, it is interesting to know that what ZooKeeper actually does");
        System.out.println("is retrieving all IP addresses of all hosts listed in the ZK connection");
        System.out.println("string, and then it picks one out of these to connect to.");
        System.out.println("These are all the IP-addresses coupled to " + cn + ":");
        InetAddress addrs[] = InetAddress.getAllByName(cn);
        for (InetAddress addr : addrs) {
            System.out.println("  " + addr.getHostAddress());
        }
        
        return 0;
    }
}
