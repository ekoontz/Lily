package org.lilycms.rowlog.impl;

import java.util.List;

import org.lilycms.rowlog.api.SubscriptionContext;

public interface SubscriptionsWatcherCallBack {

    void subscriptionsChanged(List<SubscriptionContext> subscriptions);
}
