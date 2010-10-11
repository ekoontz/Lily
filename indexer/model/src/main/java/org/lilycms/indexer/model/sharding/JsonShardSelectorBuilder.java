package org.lilycms.indexer.model.sharding;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.util.json.JsonFormat;
import org.lilycms.util.json.JsonUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Creates a {@link ShardSelector} from a json config.
 *
 * <pre>
 * {
 *   shardingKey: {
 *     value: {
 *       source: "recordId|masterRecordId|variantProperty",
 *       property: "prop name" [only if source = variantProperty]
 *     }
 *     type: "long|string",
 *     hash: "md5", [optional, only if you want the value to be hashed]
 *     modulus: 3, [optional, only possible if type is long]
 *   },
 *
 *   mapping: {
 *     type: "list|range",
 *
 *     in case of list:
 *
 *     entries: [
 *       { shard: "shard1", values: [0, 1, 2] }, values in array should be long or string according to type
 *       { shard: "shard2", values: [3, 4, 5] }
 *     ]
 *
 *     in case of range:

 *     entries: [
 *       { shard: "shard1", upTo: 1000 },
 *       { shard: "shard2" }
 *     ]
 *   }
 * }
 * </pre>
 */
public class JsonShardSelectorBuilder {

    public static ShardSelector build(byte[] configData) throws ShardingConfigException {
        JsonNode node;
        try {
            node = JsonFormat.deserializeNonStd(new ByteArrayInputStream(configData));
        } catch (IOException e) {
            throw new ShardingConfigException("Error reading the sharding configuration.", e);
        }
        if (node.isObject()) {
            return build((ObjectNode)node);
        } else {
            throw new ShardingConfigException("The sharding config should be a JSON object.");
        }
    }

    public static ShardSelector build(ObjectNode configNode) throws ShardingConfigException {
        //
        // Build the sharding key
        //

        ObjectNode shardingKeyNode = JsonUtil.getObject(configNode, "shardingKey");

        String hash = JsonUtil.getString(shardingKeyNode, "hash", null);
        boolean enableHashing = false;
        if (hash != null && hash.equalsIgnoreCase("MD5")) {
            enableHashing = true;
        } else if (hash != null) {
            throw new ShardingConfigException("Unsupported hash algorithm: " + hash);
        }

        String shardingKeyTypeName = JsonUtil.getString(shardingKeyNode, "type");
        ShardingKey.KeyType keyType;
        if (shardingKeyTypeName.equalsIgnoreCase("string")) {
            keyType = ShardingKey.KeyType.STRING;
        } else if (shardingKeyTypeName.equalsIgnoreCase("long")) {
            keyType = ShardingKey.KeyType.LONG;
        } else {
            throw new ShardingConfigException("Invalid sharding key type: " + shardingKeyTypeName);
        }

        int modulus = JsonUtil.getInt(shardingKeyNode, "modulus", 0);
        if (modulus > 0 && keyType != ShardingKey.KeyType.LONG) {
            throw new ShardingConfigException("modulus is only allowed if sharding key type is long");
        }


        ObjectNode shardingValue = JsonUtil.getObject(shardingKeyNode, "value");
        String shardingValueSource = JsonUtil.getString(shardingValue, "source");

        ShardingKey shardingKey;
        if (shardingValueSource.equals("masterRecordId")) {
            shardingKey = ShardingKey.masterRecordIdShardingKey(enableHashing, modulus, keyType);
        } else if (shardingValueSource.equals("recordId")) {
            shardingKey = ShardingKey.recordIdShardingKey(enableHashing, modulus, keyType);
        } else if (shardingValueSource.equals("variantProperty")) {
            String property = JsonUtil.getString(shardingValue, "property");
            shardingKey = ShardingKey.variantProperyShardingKey(property, enableHashing, modulus, keyType);
        } else {
            throw new ShardingConfigException("Invalid sharding key value source: " + shardingValueSource);
        }

        //
        // Build the mapping
        //

        ObjectNode mappingNode = JsonUtil.getObject(configNode, "mapping");
        String mappingTypeName = JsonUtil.getString(mappingNode, "type");

        ShardSelector selector;
        if (mappingTypeName.equals("range")) {
            selector = new RangeShardSelector(shardingKey);
        } else if (mappingTypeName.equals("list")) {            
            selector = new ListShardSelector(shardingKey);
        } else {
            throw new ShardingConfigException("Unsupported mappingType: " + mappingTypeName);
        }

        ArrayNode mappingEntriesNode = JsonUtil.getArray(mappingNode, "entries");

        if (mappingEntriesNode.size() == 0) {
            throw new ShardingConfigException("Mapping does not contain any entries.");
        }

        for (int i = 0; i < mappingEntriesNode.size(); i++) {
            JsonNode node = mappingEntriesNode.get(i);
            if (!node.isObject()) {
                throw new ShardingConfigException("The entries within the mapping array should be json objects.");
            }
            String shardName = JsonUtil.getString(node, "shard");

            if (mappingTypeName.equals("range")) {
                Comparable upToValue = null;
                JsonNode upTo = node.get("upTo");
                if (upTo == null && i != mappingEntriesNode.size() - 1) {
                    throw new ShardingConfigException("upTo value is not specified in mapping for the non-last" +
                            " mapping, shard name: " + shardName);
                } else if (upTo == null) {
                    // ok, can be null for last mapping to indicate unbounded range
                } else {
                    upToValue = getValue(keyType, upTo);
                }
                ((RangeShardSelector)selector).addMapping(shardName, upToValue);
            } else {
                ArrayNode valuesNode = JsonUtil.getArray(node, "values");
                for (int j = 0; j < valuesNode.size(); j++) {
                    Comparable value = getValue(keyType, valuesNode.get(j));
                    ((ListShardSelector)selector).addMapping(value, shardName);
                }
            }
        }

        return selector;
    }

    private static Comparable getValue(ShardingKey.KeyType keyType, JsonNode value) throws ShardingConfigException {
        Comparable result;

        switch (keyType) {
            case STRING:
                if (value.isTextual()) {
                    result = value.getTextValue();
                } else {
                    throw new ShardingConfigException("Values specified in the mapping should correspond to the" +
                            " sharding key type, in this case string.");
                }
                break;
            case LONG:
                if (value.isLong() || value.isInt()) {
                    result = value.getLongValue();
                } else {
                    throw new ShardingConfigException("Values specified in the mapping should correspond to the" +
                            " sharding key type, in this case long.");
                }
                break;
            default:
                throw new RuntimeException("Unexpected sharding key type: " + keyType);
        }

        return result;
    }
}
