package org.lilyproject.tools.recordrowvisualizer;

public class ExecutionData {
    protected String subscriptionId;
    protected int tryCount;
    protected boolean success;
    protected String lock;

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public int getTryCount() {
        return tryCount;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getLock() {
        return lock;
    }
}
