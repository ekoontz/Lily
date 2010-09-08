package org.lilycms.indexer.model.impl;

import net.iharder.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.indexer.model.api.ActiveBuildJobInfo;
import org.lilycms.indexer.model.api.BuildJobInfo;
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

        ActiveBuildJobInfo activeBuildJob = null;
        if (node.get("activeBuildJob") != null) {
            ObjectNode jobNode = JsonUtil.getObject(node, "activeBuildJob");
            activeBuildJob = new ActiveBuildJobInfo();
            activeBuildJob.setJobId(JsonUtil.getString(jobNode, "jobId"));
            activeBuildJob.setSubmitTime(JsonUtil.getLong(jobNode, "submitTime"));
        }

        BuildJobInfo lastBuildJob = null;
        if (node.get("lastBuildJob") != null) {
            ObjectNode jobNode = JsonUtil.getObject(node, "lastBuildJob");
            lastBuildJob = new BuildJobInfo();
            lastBuildJob.setJobId(JsonUtil.getString(jobNode, "jobId"));
            lastBuildJob.setSubmitTime(JsonUtil.getLong(jobNode, "submitTime"));
            lastBuildJob.setSuccess(JsonUtil.getBoolean(jobNode, "success"));
            lastBuildJob.setJobState(JsonUtil.getString(jobNode, "jobState"));
        }

        index.setState(state);
        index.setMessageConsumerId(messageConsumerId);
        index.setConfiguration(configuration);
        index.setSolrShards(solrShards);
        index.setActiveBuildJobInfo(activeBuildJob);
        index.setLastBuildJobInfo(lastBuildJob);
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

        if (index.getActiveBuildJobInfo() != null) {
            ActiveBuildJobInfo jobInfo = index.getActiveBuildJobInfo();
            ObjectNode jobNode = node.putObject("activeBuildJob");
            jobNode.put("jobId", jobInfo.getJobId());
            jobNode.put("submitTime", jobInfo.getSubmitTime());
        }

        if (index.getLastBuildJobInfo() != null) {
            BuildJobInfo jobInfo = index.getLastBuildJobInfo();
            ObjectNode jobNode = node.putObject("lastBuildJob");
            jobNode.put("jobId", jobInfo.getJobId());
            jobNode.put("submitTime", jobInfo.getSubmitTime());
            jobNode.put("success", jobInfo.getSuccess());
            jobNode.put("jobState", jobInfo.getJobState());
        }

        return node;
    }
}
