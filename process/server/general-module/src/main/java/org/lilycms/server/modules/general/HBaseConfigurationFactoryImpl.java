package org.lilycms.server.modules.general;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfigurationFactoryImpl implements HBaseConfigurationFactory {
    private String zkQuorum;
    private String zkClientPort;

    public HBaseConfigurationFactoryImpl(String zkQuorum, String zkClientPort) {
        this.zkQuorum = zkQuorum;
        this.zkClientPort = zkClientPort;
    }

    public Configuration get() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkQuorum);
        config.set("hbase.zookeeper.property.clientPort", zkClientPort);
        return config;
    }
}
