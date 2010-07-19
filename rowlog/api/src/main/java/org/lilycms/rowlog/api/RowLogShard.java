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

/**
 * A RowLogShard manages the actual RowLogMessages on a HBase table. It needs to be registered to a RowLog.
 * 
 * <p> This API will be changed so that the putMessage can be called once for all related consumers.
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
     * Removes the RowLogMessage from the table for the indicated consumer.
     * 
     * @param message the {@link RowLogMessage} to be removed from the table
     * @param consumerId the id of the {@link RowLogConsumer} for which the message needs to be removed
     * @throws RowLogException when an unexpected exception occurs
     */
    void removeMessage(RowLogMessage message, int consumerId) throws RowLogException;
    
    /**
     * Retrieves the next message to be processed by the indicated consumer.
     * 
     * @param consumerId the id of the {@link RowLogConsumer} for which the next message should be retrieved
     * @return the next {@link RowLogMessage} to be processed
     * @throws RowLogException when an unexpected exception occurs
     */
    RowLogMessage next(int consumerId) throws RowLogException;
}
