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
package org.lilyproject.rowlog.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;

public class RowLogProcessorNotifier {
    
    private RowLogConfigurationManager rowLogConfigurationManager;
    private Map<String, Long> wakeupDelays = Collections.synchronizedMap(new HashMap<String, Long>());
    private long delay = 100;
    private Log log = LogFactory.getLog(getClass());

    public RowLogProcessorNotifier(RowLogConfigurationManager rowLogConfigurationManager) {
        this.rowLogConfigurationManager = rowLogConfigurationManager;
    }
    
    protected void notifyProcessor(String rowLogId, String shardId) throws InterruptedException {
        long now = System.currentTimeMillis();
        Long delayUntil = wakeupDelays.get(rowLogId+shardId);
        if (delayUntil == null || now >= delayUntil) {
            sendNotification(rowLogId, shardId);
            // Wait at least 100ms before sending another notification 
            wakeupDelays.put(rowLogId+shardId, now + delay);
        }
    }

	private void sendNotification(String rowLogId, String shardId)
			throws InterruptedException {
		try {
			rowLogConfigurationManager.notifyProcessor(rowLogId, shardId);
		} catch (KeeperException e) {
			log.debug("Exception while notifying processor of rowLog <"+ rowLogId+"> and shard <"+shardId+">", e);
		}
	}
    
    public void close() {
    }
    
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
