/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.rowlog.api;

import org.apache.zookeeper.KeeperException;
import org.lilyproject.rowlog.api.RowLogSubscription.Type;

public interface RowLogConfigurationManager {

    /**
     * Adds a rowlog and its configuration parameters. These configuration parameters are needed by the rowlog and rowlog processor.
     * It is advised to add these parameters to the configuration manager before starting the rowlog and rowlog processors.
     * 
     * <p> This method blocks if the ZooKeeper connection is down.
     * 
     * <p> If the rowlog would already exist, this method will update the configuration paramaters.
     * 
     * @param rowLogId the id of the rowlog to add
     * @param rowLogConfig the configuration parameters
     */
    void addRowLog(String rowLogId, RowLogConfig rowLogConfig) throws KeeperException, InterruptedException;
    
    /**
     * Updates the rowlog configuration parameters.
     * 
     * <p>This method blocks if the ZK connection is down.
     * 
     * @param rowLogId the id of the rowlog to update
     * @param rowLogConfig the new configuration paramters
     */
    void updateRowLog(String rowLogId, RowLogConfig rowLogConfig) throws KeeperException, InterruptedException;
    
    /**
     * Removes the rowlog and its configuration parameters.
     * 
     * <p>This method blocks if the ZK connection is down.
     * 
     * <p> All subscriptions need to be removed before the rowlog can be removed.
     * <p> Any rowLogObservers should be stopped first or should be able to handle the removal. 
     * 
     * @param rowLogId the id of the rowlog to remove
     * @throws RowLogException thrown when the rowlog still has subscriptions registered
     */
    void removeRowLog(String rowLogId) throws KeeperException, InterruptedException, RowLogException;
    
    boolean rowLogExists(String rowLogId) throws InterruptedException, KeeperException;
    
    /**
     * Add a new rowlog observer. After registration, the observer will asynchronously be called to
     * report the initial rowlog configuration parameters.
     */
    void addRowLogObserver(String rowLogId, RowLogObserver observer);
    void removeRowLogObserver(String rowLogId, RowLogObserver observer);

    
    /**
     * Adds a subscription.
     *
     * <p>This method blocks if the ZooKeeper connection is down.
     *
     * <p>If the subscription would already exist, this method will update the subscription. 
     * 
     * <p>Due to the nature of the implementation of this method, it is difficult to know if it was really this process which created the node.
     * Note that there is also a chance the current process is interrupted or dies after the subscription is
     * created but before this method returned. It might also be that someone else removes the subscription again
     * by the time this method returns into your code. Therefore, the advice is that subscriptionId's should be
     * selected such that it does not matter if the subscription already existed, but only that the outcome is
     * 'a subscription with this id exists'.
     * @param rowLogId the id of the rowlog to add the subscription to
     * @param subscriptionId the id of the subscription to add
     * @param type to indicate wether the listeners of the subscription will run locally (VM) or remote (Netty)
     * @param maxTries the number of times to try processing a message for this subscription before marking it as problematic
     * @param orderNr a number to order the subscription wrt the other subscriptions
     */
    void addSubscription(String rowLogId, String subscriptionId, Type type, int maxTries, int orderNr) throws KeeperException,
            InterruptedException, RowLogException;

    /**
     * Updates a subscription.
     * 
     * <p>This method blocks if the ZooKeeper connection is down.
     * @param rowLogId the id of the rowlog to add the subscription to
     * @param subscriptionId the id of the subscription to add
     * @param type to indicate wether the listeners of the subscription will run locally (VM) or remote (Netty)
     * @param maxTries the number of times to try processing a message for this subscription before marking it as problematic
     * @param orderNr a number to order the subscription wrt the other subscriptions
     * @throws RowLogException thrown when the subscription does not exist
     */
    void updateSubscription(String rowLogId, String subscriptionId, Type type, int maxTries, int orderNr) throws KeeperException, InterruptedException, RowLogException;
    
    /**
     * Deletes a subscription.
     *
     * <p>This method blocks if the ZK connection is down.
     *
     * <p>If the subscription would not exist, this method silently returns.
     */
    void removeSubscription(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException, RowLogException;

    boolean subscriptionExists(String rowLogId, String subscriptionId) throws InterruptedException, KeeperException;

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

    /**
     * Add a new processor notify obeserver.
     */
    void addProcessorNotifyObserver(String rowLogId, String shardId, ProcessorNotifyObserver observer);
    
    void removeProcessorNotifyObserver(String rowLogId, String shardId);
    
    /**
     *
     * <p>This method blocks if the ZK connection is down.
     *
     * <p>If the listener would already exist, this method silently returns.
     * @param listenerId an id that serves to identify the listener by a {@link ListenerSubscriptionHandler}. 
     * The {@link RemoteListenerSubscriptionHandler} for instance identifies the listener by host and port of where
     * the remote listener is running.
     */
    void addListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException, InterruptedException, KeeperException;

    /**
     *
     * <p>This method blocks if the ZK connection is down.
     *
     * <p>If the listener would not exist, this method silently returns.
     */
    void removeListener(String rowLogId, String subscriptionId, String listenerId) throws RowLogException, InterruptedException, KeeperException;

    /**
     * Notify the processor that a new message has been put on the rowlog.
     * <p>If the processor was in a wait mode, it will wake up and check the rowlog for new messages.
     */
	void notifyProcessor(String rowLogId, String shardId)
			throws InterruptedException, KeeperException;

}
