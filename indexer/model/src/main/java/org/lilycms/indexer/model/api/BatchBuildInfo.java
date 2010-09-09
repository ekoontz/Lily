package org.lilycms.indexer.model.api;

import org.lilycms.util.ObjectUtils;

public class BatchBuildInfo {
    private String jobId;
    private long submitTime;
    private boolean success;
    private String jobState;
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

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        checkIfMutable();
        this.success = success;
    }

    public String getJobState() {
        return jobState;
    }

    public void setJobState(String jobState) {
        checkIfMutable();
        this.jobState = jobState;
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
        BatchBuildInfo other = (BatchBuildInfo)obj;

        if (!ObjectUtils.safeEquals(jobId, other.jobId))
            return false;

        if (submitTime != other.submitTime)
            return false;

        if (success != other.success)
            return false;

        if (!ObjectUtils.safeEquals(jobState, other.jobState))
            return false;

        return true;
    }
}
