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
 * A RowLogMessageConsumer is responsible for processing a message coming from the {@link RowLog}.
 * 
 * <p> RowLogMessageConsumers should be registered on the {@link RowLog}. 
 * A consumer can be asked to process a message, either by the {@link RowLog} itself when it is asked to process a message,
 * or by a {@link RowLogProcessor} that picks up a next message to be processed by a specific consumer.
 * A {@link RowLogProcessor} will only ask a consumer to process a message if the RowLogMessageConsumer was registered
 * with the {@link RowLog} before the message was put on the {@link RowLog}. 
 *
 */
public interface RowLogMessageListener {

    /**
     * An id which should be unique across all registered consumers on the {@link RowLog} 
     */
    int getId();
    
    /**
     * Indicates the number of times the processing of a message should be tried before it should be marked as problematic. 
     */
    int getMaxTries();

    /**
     * Request a consumer to process a {@link RowLogMessage}. 
     * @param message the {@link RowLogMessage} to process
     * @return true if the consumer could successfully process the {@link RowLogMessage}
     */
    boolean processMessage(RowLogMessage message);

}
