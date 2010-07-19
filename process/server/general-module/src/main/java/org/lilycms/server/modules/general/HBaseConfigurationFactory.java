package org.lilycms.server.modules.general;

import org.apache.hadoop.conf.Configuration;

public interface HBaseConfigurationFactory {
    Configuration get();
}
