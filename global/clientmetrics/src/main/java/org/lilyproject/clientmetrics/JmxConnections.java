package org.lilyproject.clientmetrics;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JmxConnections {
    private Map<String, MBeanServerConnection> connections = new HashMap<String, MBeanServerConnection>();

    public MBeanServerConnection getMBeanServer(String serverName, String port) throws IOException {
        String hostport = serverName + ":" + port;
        MBeanServerConnection connection = connections.get(hostport);
        if (connection == null) {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
            JMXConnector connector = JMXConnectorFactory.connect(url);
            connection = connector.getMBeanServerConnection();
            connections.put(serverName, connection);
        }
        return connection;
    }
}
