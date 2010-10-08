package org.lilycms.rowlog.api;

import java.util.List;

public interface SubscriptionsObserver {

    /**
     * Notifies the subscriptions have changed.
     *
     * <p>An observer will never be called from more than one thread concurrently, i.e. all
     * changes are reported sequentially.
     *
     * @param subscriptions the full list of current subscriptions.
     */
    void subscriptionsChanged(List<RowLogSubscription> subscriptions);
}
