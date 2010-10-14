package org.lilycms.indexer.model.api;

import org.lilycms.util.ObjectUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BatchBuildInfo {
    private String jobId;
    private long submitTime;
    private boolean success;
    private String jobState;
    private boolean immutable;
    private String trackingUrl;
    private Map<String, Long> counters = new HashMap<String, Long>();

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

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        checkIfMutable();
        this.trackingUrl = trackingUrl;
    }

    public Map<String, Long> getCounters() {
        return Collections.unmodifiableMap(counters);
    }

    public void addCounter(String key, long value) {
        checkIfMutable();
        counters.put(key, value);
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

        if (!ObjectUtils.safeEquals(trackingUrl, other.trackingUrl))
            return false;

        if (!ObjectUtils.safeEquals(counters, other.counters))
            return false;

        return true;
    }
}
