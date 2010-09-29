package org.lilycms.rowlog.api;

import org.apache.zookeeper.KeeperException;
import org.lilycms.rowlog.api.SubscriptionContext.Type;

public interface RowLogConfigurationManager {

    void addSubscription(String rowLogId, String subscriptionId, Type type, int maxTries) throws KeeperException,
            InterruptedException;

    void removeSubscription(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException;

}
