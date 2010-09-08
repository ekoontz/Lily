package org.lilycms.indexer.model.api;

public class BuildJobInfo {
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
}
