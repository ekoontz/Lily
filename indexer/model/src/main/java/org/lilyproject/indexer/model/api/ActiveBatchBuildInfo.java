package org.lilyproject.indexer.model.api;

import org.lilyproject.util.ObjectUtils;

public class ActiveBatchBuildInfo {
    private String jobId;
    private long submitTime;
    private boolean immutable;
    private String trackingUrl;

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

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        checkIfMutable();
        this.trackingUrl = trackingUrl;
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

        if (!ObjectUtils.safeEquals(trackingUrl, other.trackingUrl))
            return false;

        return true;
    }
}
