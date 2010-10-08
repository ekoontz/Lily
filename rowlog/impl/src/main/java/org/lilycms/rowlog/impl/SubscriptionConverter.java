package org.lilycms.rowlog.impl;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.rowlog.api.RowLogSubscription;
import org.lilycms.util.json.JsonFormat;
import org.lilycms.util.json.JsonUtil;

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
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsBytes(toJson(subscription));
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
