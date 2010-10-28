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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps subscription id's onto {@link RowLogMessageListener} instances.
 * The RowLog and RowLogProcessor retrieves these instances to process messages.
 */
public class RowLogMessageListenerMapping {

    public static RowLogMessageListenerMapping INSTANCE = new RowLogMessageListenerMapping();
    
    private Map<String, RowLogMessageListener> mapping = Collections.synchronizedMap(new HashMap<String, RowLogMessageListener>());

    private RowLogMessageListenerMapping() {
    }
    
    public void put(String id, RowLogMessageListener listenerInstance) {
        mapping.put(id, listenerInstance);
    }
    
    public RowLogMessageListener get(String id) {
        return mapping.get(id);
    }
    
    public void remove(String id) {
        mapping.remove(id);
    }
}
