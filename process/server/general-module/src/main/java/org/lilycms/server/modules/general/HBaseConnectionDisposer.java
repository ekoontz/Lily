package org.lilycms.server.modules.general;

import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;

import javax.annotation.PreDestroy;

public class HBaseConnectionDisposer {
    @PreDestroy
    public void stop() {
        try {
            HConnectionManager.deleteAllConnections(true);
        } catch (Throwable t) {
            LogFactory.getLog(getClass()).error("Problem cleaning up HBase connections", t);
        }
    }
}
