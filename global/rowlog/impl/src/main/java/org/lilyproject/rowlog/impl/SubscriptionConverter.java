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

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class SubscriptionConverter {
    public static SubscriptionConverter INSTANCE = new SubscriptionConverter();

    public RowLogSubscription fromJsonBytes(String rowLogId, String subscriptionId, byte[] json) {
        ObjectNode node;
        try {
            node = (ObjectNode)JsonFormat.deserialize(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing row log subscription JSON. Row log ID " + rowLogId +
                    ", subscription ID " + subscriptionId, e);
        }
        return fromJson(rowLogId, subscriptionId, node);
    }

    public RowLogSubscription fromJson(String rowLogId, String subscriptionId, ObjectNode node) {
        RowLogSubscription.Type type = RowLogSubscription.Type.valueOf(JsonUtil.getString(node, "type"));
        int maxTries = JsonUtil.getInt(node, "maxTries");
        int orderNr = JsonUtil.getInt(node, "orderNr");

        return new RowLogSubscription(rowLogId, subscriptionId, type, maxTries, orderNr);
    }

    public byte[] toJsonBytes(RowLogSubscription subscription) {
        try {
            return JsonFormat.serializeAsBytes(toJson(subscription));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing row log subscription to JSON. Row log ID " +
                    subscription.getRowLogId() + ", subscription ID " + subscription.getId(), e);
        }
    }

    public ObjectNode toJson(RowLogSubscription subscription) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("type", subscription.getType().toString());
        node.put("maxTries", subscription.getMaxTries());
        node.put("orderNr", subscription.getOrderNr());

        return node;
    }
}
