package org.lilycms.rowlog.api;

public class SubscriptionExistsException extends Exception {
    private final String rowLogId;
    private final String subscriptionId;

    public SubscriptionExistsException(String rowLogId, String subscriptionId) {
        this.rowLogId = rowLogId;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public String getMessage() {
        return "The row log " + rowLogId + " already has a subscription with ID " + subscriptionId;
    }
}
