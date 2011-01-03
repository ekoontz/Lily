package org.lilyproject.clientmetrics;

import java.util.Collections;
import java.util.List;

public class NullPlugin implements MetricsPlugin {
    public void beforeReport(Metrics metrics) {
    }

    public List<String> getExtraInfoLines() {
        return Collections.emptyList();
    }
}
