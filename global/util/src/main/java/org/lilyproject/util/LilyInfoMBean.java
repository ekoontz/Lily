package org.lilyproject.util;

public interface LilyInfoMBean {
    String getVersion();

    boolean isIndexerMaster();

    boolean isRowLogProcessorMQ();

    boolean isRowLogProcessorWAL();
}
