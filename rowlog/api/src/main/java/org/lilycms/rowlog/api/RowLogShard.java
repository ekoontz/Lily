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

import java.util.List;

/**
 * A RowLogShard manages the actual RowLogMessages on a HBase table. It needs to be registered to a RowLog.
 * 
 * <p> This API will be changed so that the putMessage can be called once for all related subscriptions.
 */
public interface RowLogShard {

    /**
     * The id of a RowLogShard uniquely identifies a shard in the context of a {@link RowLog}
     */
    String getId();
    
    /**
     * Puts a RowLogMessage onto the table.
     * 
     * @param message the {@link RowLogMessage} to be put on the table
     * @throws RowLogException when an unexpected exception occurs
     */
    void putMessage(RowLogMessage message) throws RowLogException;
    
    /**
     * Removes the RowLogMessage from the table for the indicated subscription.
     * 
     * <p>This also removes any messages that might have been marked as problematic.
     * 
     * @param message the {@link RowLogMessage} to be removed from the table
     * @param subscription the id of the subscription for which the message needs to be removed
     * @throws RowLogException when an unexpected exception occurs
     */
    void removeMessage(RowLogMessage message, String subscription) throws RowLogException;
    
    
    /**
     * Retrieves the next messages to be processed by the indicated subscription.
     * 
     * @param subscription the id of the subscription for which the next messages should be retrieved
     * @return the next 100, or less {@link RowLogMessage}s to be processed
     * @throws RowLogException when an unexpected exception occurs
     */
    List<RowLogMessage> next(String subscription) throws RowLogException;

    /**
     * Marks a RowLogMessage as problematic for the indicated subscription.
     * The message won't be returned when {@link #next(String)} is called.
     * See {@link #nextProblematic(String)}
     * 
     * <p>A message should be marked as problematic when it has failed to be processed after multiple retries.  
     */
    void markProblematic(RowLogMessage message, String subscription) throws RowLogException;
    
    /**
     * Retrieves the messages that have been marked as problematic for the indicated subscription.
     * @param subscription the id of the subscription for which the problematic messages should be retrieved
     * @return all {@link RowLogMessage}s that have been marked as problematic
     * @throws RowLogException when an unexpected exception occurs
     */
    List<RowLogMessage> getProblematic(String subscription) throws RowLogException;

    boolean isProblematic(RowLogMessage message, String subscription) throws RowLogException;
}
