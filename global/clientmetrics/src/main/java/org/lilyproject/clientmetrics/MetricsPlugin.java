package org.lilyproject.clientmetrics;

import java.util.List;

public interface MetricsPlugin {
    void beforeReport(Metrics metrics);

    List<String> getExtraInfoLines();
}
