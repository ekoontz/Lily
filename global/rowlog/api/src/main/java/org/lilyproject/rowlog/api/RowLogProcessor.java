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

import java.util.List;

/**
 * A RowLogProcessor is responsible for monitoring the {@link RowLog} for incoming messages 
 * and hand them over for processing to the {@link RowLogMessageListener}s that are registered with the {@link RowLog} 
 *
 * <p> More specifically, a RowLogProcessor is responsible for the messages of one {@link RowLogShard}.
 * So, one RowLogProcessor should be started for each registered {@link RowLogShard}.
 */
public interface RowLogProcessor {
    /**
     * Starts the RowLogProcessor. The execution should start in a separate thread, and the start call should return immediately. 
     * @throws InterruptedException 
     */
    void start() throws InterruptedException;
    
    /**
     * Indicate that the RowLogProcessor should stop executing as soon as possible.
     */
    void stop();
    
    /**
     * Check is the RowLogProcessor is executing
     * @return true if the RowLogProcessor is executing
     */
    boolean isRunning(int consumerId);

    void subscriptionsChanged(List<RowLogSubscription> andWatchSubscriptions);

    /**
     * The RowLog for which this processor is working.
     */
    RowLog getRowLog();
}
