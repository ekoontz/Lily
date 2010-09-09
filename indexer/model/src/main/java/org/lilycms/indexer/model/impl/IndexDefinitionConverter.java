package org.lilycms.indexer.model.impl;

import net.iharder.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.indexer.model.api.*;
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
        IndexGeneralState state = IndexGeneralState.valueOf(JsonUtil.getString(node, "generalState"));
        IndexUpdateState updateState = IndexUpdateState.valueOf(JsonUtil.getString(node, "updateState"));
        IndexBatchBuildState buildState = IndexBatchBuildState.valueOf(JsonUtil.getString(node, "batchBuildState"));

        String queueSubscriptionId = JsonUtil.getString(node, "queueSubscriptionId", null);
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

        ActiveBatchBuildInfo activeBatchBuild = null;
        if (node.get("activeBatchBuild") != null) {
            ObjectNode buildNode = JsonUtil.getObject(node, "activeBatchBuild");
            activeBatchBuild = new ActiveBatchBuildInfo();
            activeBatchBuild.setJobId(JsonUtil.getString(buildNode, "jobId"));
            activeBatchBuild.setSubmitTime(JsonUtil.getLong(buildNode, "submitTime"));
        }

        BatchBuildInfo lastBatchBuild = null;
        if (node.get("lastBatchBuild") != null) {
            ObjectNode buildNode = JsonUtil.getObject(node, "lastBatchBuild");
            lastBatchBuild = new BatchBuildInfo();
            lastBatchBuild.setJobId(JsonUtil.getString(buildNode, "jobId"));
            lastBatchBuild.setSubmitTime(JsonUtil.getLong(buildNode, "submitTime"));
            lastBatchBuild.setSuccess(JsonUtil.getBoolean(buildNode, "success"));
            lastBatchBuild.setJobState(JsonUtil.getString(buildNode, "jobState"));
        }

        index.setGeneralState(state);
        index.setUpdateState(updateState);
        index.setBatchBuildState(buildState);
        index.setQueueSubscriptionId(queueSubscriptionId);
        index.setConfiguration(configuration);
        index.setSolrShards(solrShards);
        index.setActiveBatchBuildInfo(activeBatchBuild);
        index.setLastBatchBuildInfo(lastBatchBuild);
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

        node.put("generalState", index.getGeneralState().toString());
        node.put("batchBuildState", index.getBatchBuildState().toString());
        node.put("updateState", index.getUpdateState().toString());

        if (index.getQueueSubscriptionId() != null)
            node.put("queueSubscriptionId", index.getQueueSubscriptionId());

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

        if (index.getActiveBatchBuildInfo() != null) {
            ActiveBatchBuildInfo buildInfo = index.getActiveBatchBuildInfo();
            ObjectNode buildNode = node.putObject("activeBatchBuild");
            buildNode.put("jobId", buildInfo.getJobId());
            buildNode.put("submitTime", buildInfo.getSubmitTime());
        }

        if (index.getLastBatchBuildInfo() != null) {
            BatchBuildInfo buildInfo = index.getLastBatchBuildInfo();
            ObjectNode buildNode = node.putObject("lastBatchBuild");
            buildNode.put("jobId", buildInfo.getJobId());
            buildNode.put("submitTime", buildInfo.getSubmitTime());
            buildNode.put("success", buildInfo.getSuccess());
            buildNode.put("jobState", buildInfo.getJobState());
        }

        return node;
    }
}
