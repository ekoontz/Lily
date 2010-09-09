package org.lilycms.indexer.model.api;

import org.lilycms.util.ObjectUtils;

public class ActiveBatchBuildInfo {
    private String jobId;
    private long submitTime;
    private boolean immutable;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        checkIfMutable();
        this.jobId = jobId;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(long submitTime) {
        checkIfMutable();
        this.submitTime = submitTime;
    }

    public void makeImmutable() {
        this.immutable = true;
    }

    private void checkIfMutable() {
        if (immutable)
            throw new RuntimeException("This IndexDefinition is immutable");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ActiveBatchBuildInfo other = (ActiveBatchBuildInfo)obj;

        if (!ObjectUtils.safeEquals(jobId, other.jobId))
            return false;

        if (submitTime != other.submitTime)
            return false;

        return true;
    }
}
