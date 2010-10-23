package org.lilyproject.indexer.engine;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.lilyproject.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilyproject.indexer.model.sharding.ShardSelector;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.indexer.model.sharding.ShardingConfigException;
import org.lilyproject.repository.api.RecordId;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.*;

public class SolrServers {
    private Map<String, String> shards;
    private Map<String, SolrServer> shardConnections;
    private ShardSelector selector;
    private HttpClient httpClient;

    public SolrServers(Map<String, String> shards, ShardSelector selector, HttpClient httpClient) throws MalformedURLException {
        this.shards = shards;
        this.selector = selector;
        this.httpClient = httpClient;

        init();
    }

    /**
     * This method is only meant for use by test cases.
     */
    public static SolrServers createForOneShard(String uri) throws URISyntaxException, ShardingConfigException,
            MalformedURLException {
        SortedMap<String, String> shards = new TreeMap<String, String>();
        shards.put("shard1", uri);
        ShardSelector selector = DefaultShardSelectorBuilder.createDefaultSelector(shards);
        return new SolrServers(shards, selector, new HttpClient(new MultiThreadedHttpConnectionManager()));
    }

    /**
     * This method is only meant for use by test cases.
     */
    public void commit(boolean waitFlush, boolean waitSearcher) throws IOException, SolrServerException {
        for (SolrServer server : shardConnections.values()) {
            server.commit(waitFlush, waitSearcher);
        }
    }

    /**
     * This method is only meant for use by test cases. Currently queries the first shard only.
     */
    public QueryResponse query(SolrQuery query) throws SolrServerException {
        return shardConnections.values().iterator().next().query(query);
    }

    private void init() throws MalformedURLException {
        shardConnections = new HashMap<String, SolrServer>();
        for (Map.Entry<String, String> shard : shards.entrySet()) {
            shardConnections.put(shard.getKey(), new CommonsHttpSolrServer(shard.getValue(), httpClient));
        }
    }

    public SolrServer getSolrServer(RecordId recordId) throws ShardSelectorException {
        String shardName = selector.getShard(recordId);
        return shardConnections.get(shardName);
    }
}
