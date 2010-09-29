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
package org.lilycms.rowlog.api;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

/**
 * The RowLog helps managing the execution of synchronous and asynchronous actions in response to
 * updates happening to the rows of an HBase table.
 * 
 * <p> It has been introduced as a basis to build a distributed persistent Write Ahead Log (WAL) and Message Queue (MQ) 
 * on top of HBase for the Lily CMS. More information about the design and rationale behind it can be found 
 * on <a href="http://lilycms.org/">http://lilycms.org/</a>
 *
 * <p> The RowLog accepts and stores {@link RowLogMessage}s. The context of these messages is always related to a specific 
 * row in a HBase table, hence the name 'RowLog'. {@link RowLogMessageListener}s are responsible for processing the messages and
 * should be registered with the RowLog. 
 * 
 * <p> The messages are stored on, and distributed randomly over several {@link RowLogShard}s. 
 * Each shard uses one HBase table to store its messages on. And each shard is considered to be equal. It does not matter
 * if a message is stored on one or another shard.
 * (Note: the use of more than one shard is still to be implemented.) 
 * 
 * <P> For each shard, a {@link RowLogProcessor} can be started. This processor will, for each consumer, pick the next
 * message to be consumed and feed it to the {@link RowLogMessageListener} for processing. Since messages are distributed
 * over several shards and each shard has its own processor, it can happen that the order in which the messages have been
 * put on the RowLog is not respected when they are sent to the consumers for processing. See below, on how to deal with
 * this.
 *  
 * <p> On top of this, a 'payload' and 'execution state' is stored for each message in the same row to which the message relates,
 * next to the row's 'main data'.
 * The payload contains all data that is needed by a consumer to be able to process a message.
 * The execution state indicates for a message which consumers have processed the message and which consumers still need to process it.
 * 
 * <p> All messages related to a certain row are given a sequence number in the order in which they are put on the RowLog.
 * This sequence number is used when storing the payload and execution state on the HBase row. 
 * This enables a {@link RowLogMessageListener} to check if the message it is requested to process is the oldest
 * message to be processed for the row or if there are other messages to be processed for the row. It can then choose
 * to, for instance, process an older message first or even bundle the processing of multiple messages together.
 * (Note: utility methods to enable this behavior are still to be implemented on the RowLog.)    
 */
public interface RowLog {
    
    /**
     * The id of a RowLog uniquely identifies the rowlog amongst all rowlog instances.
     */
    String getId();
    
    /**
     * Registers a shard on the RowLog. (Note: the current implementation only allows for one shard to be registered.)
     * @param shard a {@link RowLogShard} 
     */
    void registerShard(RowLogShard shard);
    
    /**
     * Unregisters a shard from the RowLog.
     * @param shard a {@link RowLogShard} 
     */
    void unRegisterShard(RowLogShard shard);
    
    /**
     * Retrieves the payload of a {@link RowLogMessage} from the RowLog.
     * The preferred way to get the payload for a message is to request this through the message itself 
     * with the call {@link RowLogMessage#getPayload()} .
     * @param message a {link RowLogMessage}
     * @return the payload of the message
     * @throws RowLogException
     */
    byte[] getPayload(RowLogMessage message) throws RowLogException;
    /**
     * Puts a new message on the RowLog. This will add a new message on a {@link RowLogShard} 
     * and put the payload and an execution state on the HBase row this message is about.
     * @param rowKey the HBase row the message is related to
     * @param data some informative data to be put on the message
     * @param payload the information needed by a {@link RowLogMessageListener} to be able to process the message
     * @param put a HBase {@link Put} object that, when given, will be used by the RowLog to put the payload and 
     * execution state information on the HBase row. This enables the combination of the actual row data and the payload
     * and execution state to be put on the HBase row with a single, atomic, put call. 
     * @return a new {@link RowLogMessage} with a unique id and a sequence number indicating its position in the list of
     * messages of the row.
     * @throws RowLogException
     */
    RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException;
    
    /**
     * Request each registered {@link RowLogMessageListener} to process a {@link RowLogMessage} explicitly. 
     * This method can be called independently from a {@link RowLogProcessor} and can be used for instance when a message
     * needs to be processed immediately after it has been put on the RowLog instead of waiting for a {@link RowLogProcessor}
     * to pick it up and process it.
     * <p>This call increases the try count of the message for each registered consumer. If the maximum allowed number of
     * tries has been reached for a consumer, the message is marked as problematic for that consumer. A {@link RowLogProcessor}
     * will no longer pick up the message for that consumer. 
     * @param message a {@link RowLogMessage} to be processed
     * @return true if all consumers have processed the {@link RowLogMessage} successfully
     * @throws RowLogException
     */
    boolean processMessage(RowLogMessage message) throws RowLogException;
    
    /**
     * Locks a {@link RowLogMessage} for a certain subscription. This lock can be used if a subscription wants
     * to indicate that it is busy processing a message. 
     * The lock can be released either by calling {@link #unlockMessage(RowLogMessage, String, byte[])}, 
     * {@link #messageDone(RowLogMessage, String, byte[])}, or when the lock's timeout expires.
     * This lock only locks the message for a certain subscription, other subscriptions can still process the message in parallel.
     * <p>This call increases the try count of the message for this subscription.
     * @param message the {@link RowLogMessage} for which to take the lock
     * @param subscriptionId the id of the subscription for which to lock the message
     * @return a lock when the message was successfully locked or null when locking the message failed for instance when
     * it was locked by another instance of the same consumer
     * @throws RowLogException
     */
    byte[] lockMessage(RowLogMessage message, String subscriptionId) throws RowLogException;

    /**
     * Unlocks a {@link RowLogMessage} for a certain subscription 
     * <p>This checks the try count of the message. If the maximum amount of tries allowed for the subscription has been reached,
     * the message will be marked as problematic. 'Problematic' messages will no longer be picked up by a {@link RowLogProcessor}
     * for processing.
     * @param message the {@link RowLogMessage} for which to release the lock
     * @param subscriptionId the id of the subscription for which to release the lock
     * @param lock the lock that was received when calling {@link #lockMessage(RowLogMessage, String)}
     * @return true if releasing the lock was successful. False if releasing the lock failed, for instance because the
     * given lock does not match the lock that is currently on the message 
     * @throws RowLogException
     */
    boolean unlockMessage(RowLogMessage message, String subscriptionId, byte[] lock) throws RowLogException;
    
    /**
     * Checks if a {@link RowLogMessage} is locked for a certain subscription
     * @param message the {@link RowLogMessage} for which to check if there is a lock present
     * @param subscriptionId the id of the subscription for which to check if there is a lock present
     * @return true if a lock is present on the message
     * @throws RowLogException
     */
    boolean isMessageLocked(RowLogMessage message, String subscriptionId) throws RowLogException;
    
    /**
     * Indicates that a {@link RowLogMessage} is done for a certain subscription
     * and should not be processed anymore. This will remove the message for a certain subscription from the {@link RowLogShard} 
     * and update the execution state of this message on the row. When the execution state for all subscriptions is put to
     * done, the payload and execution state will be removed from the row.
     * A message can only be put to done when the given lock matches the lock that is present on the message, this lock
     * will then also be released.  
     * @param message the {@link RowLogMessage} to be put to done for a certain subscription
     * @param subscriptionId the id of the subscription for which to put the message to done
     * @param lock the lock that should match the lock that is present on the message, before it can be put to done
     * @return true if the message has been successfully put to done
     * @throws RowLogException
     */
    boolean messageDone(RowLogMessage message, String subscriptionId, byte[] lock) throws RowLogException;
    
    boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException;
    
    /**
     * Return all messages that are still exist for the row, or if one or more subscriptions is given, 
     * only the messages that are still open for one or more of those subscriptions.
     * <p>This call ignores if messages have been marked as problematic.
     * @param rowKey the row for which to return the messages
     * @param subscriptionId one or more subscriptions for which to return the messages that are open
     * @return a list of (open)messages of the row
     * @throws RowLogException
     */
    List<RowLogMessage> getMessages(byte[] rowKey, String ... subscriptionId) throws RowLogException;

    /**
     * Return all messages that have been marked as problematic for a certain subscription.
     * <p>Messages have a try count for each subscription for the number of times the message has been 
     * processed unsuccessfully. This try count is increased either when calling {@link #processMessage(RowLogMessage)}
     * or {@link #lockMessage(RowLogMessage, String)}. When the try count reaches the maximum allowed
     * by the subscription it is marked as 'problematic'. Problematic messages are no longer picked up
     * by a {@link RowLogProcessor}, but can still be processed by calling {@link #processMessage(RowLogMessage)}.
     * @param subscriptionId the id of the subscription
     * @return a list of problematic messages
     * @throws RowLogException
     */
    List<RowLogMessage> getProblematic(String subscriptionId) throws RowLogException;
    
    boolean isProblematic(RowLogMessage message, String subscriptionId) throws RowLogException;
    
    Collection<String> getSubscriptionIds();

    List<RowLogShard> getShards();
}
