package org.lilycms.indexer.model.impl;

import net.iharder.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.indexer.model.api.IndexDefinition;
import org.lilycms.indexer.model.api.IndexState;
import org.lilycms.util.json.JsonFormat;
import org.lilycms.util.json.JsonUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexDefinitionConverter {
    public static IndexDefinitionConverter INSTANCE = new IndexDefinitionConverter();

    public void fromJsonBytes(byte[] json, IndexDefinitionImpl index) {
        ObjectNode node;
        try {
            node = (ObjectNode)JsonFormat.deserialize(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing index definition JSON.", e);
        }
        fromJson(node, index);
    }

    public void fromJson(ObjectNode node, IndexDefinitionImpl index) {
        IndexState state = IndexState.valueOf(JsonUtil.getString(node, "state"));
        String messageConsumerId = JsonUtil.getString(node, "messageConsumerId", null);
        String configurationAsString = JsonUtil.getString(node, "configuration");
        byte[] configuration;
        try {
            configuration = Base64.decode(configurationAsString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> solrShards = new ArrayList<String>();
        ArrayNode shardsArray = JsonUtil.getArray(node, "solrShards");
        for (int i = 0; i < shardsArray.size(); i++) {
            solrShards.add(shardsArray.get(i).getTextValue());
        }

        index.setState(state);
        index.setMessageConsumerId(messageConsumerId);
        index.setConfiguration(configuration);
        index.setSolrShards(solrShards);
    }

    public byte[] toJsonBytes(IndexDefinition index) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsBytes(toJson(index));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing index definition to JSON.", e);
        }
    }

    public ObjectNode toJson(IndexDefinition index) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("state", index.getState().toString());

        if (index.getMessageConsumerId() != null)
            node.put("messageConsumerId", index.getMessageConsumerId());

        String configurationAsString;
        try {
            configurationAsString = Base64.encodeBytes(index.getConfiguration(), Base64.GZIP);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        node.put("configuration", configurationAsString);

        ArrayNode shardsNode = node.putArray("solrShards");
        for (String shard : index.getSolrShards()) {
            shardsNode.add(shard);
        }

        return node;
    }
}
