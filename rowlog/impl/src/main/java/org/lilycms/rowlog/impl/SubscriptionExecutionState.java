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
package org.lilycms.rowlog.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class SubscriptionExecutionState {

    private final byte[] messageId;
    Map<String, Boolean> states = new HashMap<String, Boolean>();
    Map<String, Integer> tryCounts = new HashMap<String, Integer>();
    Map<String, byte[]> locks = new HashMap<String, byte[]>();

    public SubscriptionExecutionState(byte[] messageId) {
        this.messageId = messageId;
    }
    
    public byte[] getMessageId() {
        return messageId;
    }
    
    public void setState(String subscription, boolean state) {
        states.put(subscription, state);
    }
    
    public boolean getState(String subscription) {
        Boolean state = states.get(subscription);
        if (state == null) return true;
        return state;
    }
    
    public void incTryCount(String subscription) {
        Integer count = tryCounts.get(subscription);
        
        if (count == null)
            tryCounts.put(subscription, 0);
        else 
            tryCounts.put(subscription, ++count);
    }
    
    public int getTryCount(String subscription) {
        Integer count = tryCounts.get(subscription);
        if (count == null) return 0;
        return count;
    }
    
    public void setLock(String subscription, byte[] lock) {
        locks.put(subscription, lock);
    }
    
    public byte[] getLock(String subscription) {
        return locks.get(subscription);
    }

    public byte[] toBytes() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode object = factory.objectNode();

        object.put("id", messageId);
        
        ArrayNode consumerStatesNode = object.putArray("states");
        for (Entry<String, Boolean> entry : states.entrySet()) {
            ObjectNode consumerStateNode = factory.objectNode();
            consumerStateNode.put("id", entry.getKey());
            consumerStateNode.put("state", entry.getValue());
            consumerStatesNode.add(consumerStateNode);
        }

        ArrayNode consumerTryCountsNode = object.putArray("counts");
        for (Entry<String, Integer> entry : tryCounts.entrySet()) {
            ObjectNode consumerTryCountNode = factory.objectNode();
            consumerTryCountNode.put("id", entry.getKey());
            consumerTryCountNode.put("count", entry.getValue());
            consumerTryCountsNode.add(consumerTryCountNode);
        }

        ArrayNode consumerLocksNode = object.putArray("locks");
        for (Entry<String, byte[]> entry : locks.entrySet()) {
            ObjectNode consumerLockNode = factory.objectNode();
            consumerLockNode.put("id", entry.getKey());
            consumerLockNode.put("lock", entry.getValue());
            consumerLocksNode.add(consumerLockNode);
        }
        
        return toJsonBytes(object);
    }
    
    public byte[] toJsonBytes(JsonNode jsonNode) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(os, jsonNode);
        } catch (IOException e) {
            // Small chance of this happening, since we are writing to a byte array
            throw new RuntimeException(e);
        }
        return os.toByteArray();
    }
    
    public static SubscriptionExecutionState fromBytes(byte[] bytes) throws IOException {
        
        JsonNode node = new ObjectMapper().readValue(bytes, 0, bytes.length, JsonNode.class);
        SubscriptionExecutionState executionState = new SubscriptionExecutionState(node.get("id").getBinaryValue());
        
        JsonNode consumerStatesNode = node.get("states");
        for (int i = 0; i < consumerStatesNode.size(); i++) {
            JsonNode consumerStateNode = consumerStatesNode.get(i);
            String id = consumerStateNode.get("id").getTextValue();
            Boolean state = consumerStateNode.get("state").getBooleanValue();
            executionState.setState(id, state);
        }

        JsonNode consumerTryCountsNode = node.get("counts");
        for (int i = 0; i < consumerTryCountsNode.size(); i++) {
            JsonNode consumerTryCountNode = consumerTryCountsNode.get(i);
            String id = consumerTryCountNode.get("id").getTextValue();
            Integer tryCount = consumerTryCountNode.get("count").getIntValue();
            executionState.tryCounts.put(id, tryCount);
        }
        
        JsonNode consumerLocksNode = node.get("locks");
        for (int i = 0; i < consumerLocksNode.size(); i++) {
            JsonNode consumerLockNode = consumerLocksNode.get(i);
            String id = consumerLockNode.get("id").getTextValue();
            byte[] lock = consumerLockNode.get("lock").getBinaryValue();
            executionState.setLock(id, lock);
        }
        
        return executionState;
    }

    public boolean allDone() {
        for (Boolean consumerDone : states.values()) {
            if (!consumerDone)
                return false;
        }
        return true;
    }
}
