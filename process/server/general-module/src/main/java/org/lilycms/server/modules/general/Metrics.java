package org.lilycms.server.modules.general;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.kauriproject.conf.Conf;

import java.io.IOException;

public class Metrics {
    public Metrics(Conf conf) throws IOException {
        ContextFactory contextFactory = ContextFactory.getFactory();

        for (Conf attr : conf.getChild("hadoopMetricsAttributes").getChildren("attribute")) {
            contextFactory.setAttribute(attr.getAttribute("name"), attr.getAttribute(("value")));
        }

        boolean enableJvmMetrics = conf.getChild("enableJvmMetrics").getValueAsBoolean();
        if (enableJvmMetrics) {
            JvmMetrics.init("lily", "aLilySession");
        }
    }
}
