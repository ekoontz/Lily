package org.lilycms.server.modules.repository;

import org.apache.hadoop.net.DNS;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class AddressResolver {
    private String interface_;
    private String nameserver;
    
    private String hostName;
    private String hostAddress;

    public AddressResolver(String interface_, String nameserver) throws UnknownHostException {
        hostName = DNS.getDefaultHost(interface_, nameserver);
        hostAddress = new InetSocketAddress(hostName, 1).getAddress().getHostAddress();
    }

    public String getHostName() {
        return hostName;
    }

    public String getHostAddress() {
        return hostAddress;
    }
}
