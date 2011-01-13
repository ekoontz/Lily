package org.lilyproject.util;

public interface LilyInfo extends LilyInfoMBean {
    void setIndexerMaster(boolean indexerMaster);

    void setRowLogProcessorMQ(boolean rowLogProcessorMQ);

    void setRowLogProcessorWAL(boolean rowLogProcessorWAL);
}
