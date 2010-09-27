package org.lilycms.rowlog.api;

import org.apache.zookeeper.KeeperException;
import org.lilycms.rowlog.api.SubscriptionContext.Type;

public interface RowLogConfigurationManager {

    void addSubscription(String rowLogId, int subscriptionId, Type type) throws KeeperException,
            InterruptedException;

    void removeSubscription(String rowLogId, int subscriptionId) throws InterruptedException, KeeperException;

}
