package org.lilyproject.server.modules.general;

import org.lilyproject.util.LilyInfo;

public class LilyInfoImpl implements LilyInfo {
    private boolean indexerMaster;
    private boolean rowLogProcessorMQ;
    private boolean rowLogProcessorWAL;

    public String getVersion() {
        return "TODO";
    }

    public boolean isIndexerMaster() {
        return indexerMaster;
    }

    public boolean isRowLogProcessorMQ() {
        return rowLogProcessorMQ;
    }

    public boolean isRowLogProcessorWAL() {
        return rowLogProcessorWAL;
    }

    public void setIndexerMaster(boolean indexerMaster) {
        this.indexerMaster = indexerMaster;
    }

    public void setRowLogProcessorMQ(boolean rowLogProcessorMQ) {
        this.rowLogProcessorMQ = rowLogProcessorMQ;
    }

    public void setRowLogProcessorWAL(boolean rowLogProcessorWAL) {
        this.rowLogProcessorWAL = rowLogProcessorWAL;
    }
}
