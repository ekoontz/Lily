package org.lilycms.rowlog.api;

import org.apache.zookeeper.KeeperException;
import org.lilycms.rowlog.api.RowLogSubscription.Type;

public interface RowLogConfigurationManager {

    /**
     *
     * <p>This method blocks if the ZK connection is down.
     */
    void addSubscription(String rowLogId, String subscriptionId, Type type, int maxTries, int orderNr) throws KeeperException,
            InterruptedException, SubscriptionExistsException;

    /**
     *
     * <p>This method blocks if the ZK connection is down.
     */
    void removeSubscription(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException;

    /**
     * Add a new subscriptions observer. After registration, the observer will asynchronously be called to
     * report the initial set of subscriptions.
     */
    void addSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer);

    void removeSubscriptionsObserver(String rowLogId, SubscriptionsObserver observer);

    /**
     * Add a new listeners observer. After registration, the observer will asynchronously be called to
     * report the initial set of listeners.
     */
    void addListenersObserver(String rowLogId, String subscriptionId, ListenersObserver observer);

    void removeListenersObserver(String rowLogId, String subscriptionId, ListenersObserver observer);

    void publishProcessorHost(String hostName, int port, String rowLogId, String shardId);

    void unPublishProcessorHost(String rowLogId, String shardId);

    String getProcessorHost(String rowLogId, String shardId);

    /**
     *
     * <p>This method blocks if the ZK connection is down.
     *
     * <p>If the listener would already exist, this method silently returns.
     */
    void addListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException, InterruptedException, KeeperException;

    /**
     *
     * <p>This method blocks if the ZK connection is down.
     *
     * <p>If the listener would not exist, this method silently returns.
     */
    void removeListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException, InterruptedException, KeeperException;

}
