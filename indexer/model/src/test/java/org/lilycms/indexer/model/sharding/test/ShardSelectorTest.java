package org.lilycms.indexer.model.sharding.test;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.lilycms.indexer.model.sharding.DefaultShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.JsonShardSelectorBuilder;
import org.lilycms.indexer.model.sharding.ShardSelector;
import org.lilycms.indexer.model.sharding.ShardSelectorException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.impl.IdGeneratorImpl;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static junit.framework.Assert.*;

public class ShardSelectorTest {
    private static final String BASE_PATH = "org/lilycms/indexer/model/sharding/test/";

    @Test
    public void testRecordIdListMapping() throws Exception {
        byte[] mapping1Data = IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream(BASE_PATH + "shardingconfig1.json"));
        ShardSelector selector = JsonShardSelectorBuilder.build(mapping1Data);

        IdGenerator idGenerator = new IdGeneratorImpl();

        String shardName = selector.getShard(idGenerator.newRecordId());
        assertNotNull(shardName);
    }

    @Test
    public void testStringFieldListMapping() throws Exception {
        byte[] mappingData = IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream(BASE_PATH + "shardingconfig2.json"));
        ShardSelector selector = JsonShardSelectorBuilder.build(mappingData);

        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.newRecordId(Collections.singletonMap("transport", "car"));

        String shardName = selector.getShard(recordId);
        assertEquals("shard1", shardName);
    }

    @Test
    public void testLongFieldRangeMapping() throws Exception {
        byte[] mappingData = IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream(BASE_PATH + "shardingconfig3.json"));
        ShardSelector selector = JsonShardSelectorBuilder.build(mappingData);

        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.newRecordId(Collections.singletonMap("weight", "400"));

        String shardName = selector.getShard(recordId);
        assertEquals("shard1", shardName);

        recordId = idGenerator.newRecordId(Collections.singletonMap("weight", "1000"));
        shardName = selector.getShard(recordId);
        assertEquals("shard2", shardName);

        recordId = idGenerator.newRecordId(Collections.singletonMap("weight", "1200"));
        shardName = selector.getShard(recordId);
        assertEquals("shard2", shardName);

        recordId = idGenerator.newRecordId(Collections.singletonMap("weight", "341234123"));
        shardName = selector.getShard(recordId);
        assertEquals("shard3", shardName);

        recordId = idGenerator.newRecordId(Collections.singletonMap("weight", "abc"));
        try {
            shardName = selector.getShard(recordId);
            fail("Expected an exception");
        } catch (ShardSelectorException e) {
            // expected
        }
    }

    @Test
    public void testDefaultMapping() throws Exception {
        SortedMap<String, String> shards = new TreeMap<String, String>();
        shards.put("shard1", "http://solr1");
        shards.put("shard2", "http://solr2");
        shards.put("shard3", "http://solr3");

        ShardSelector selector = DefaultShardSelectorBuilder.createDefaultSelector(shards);

        IdGenerator idGenerator = new IdGeneratorImpl();

        boolean shard1Used = false;
        boolean shard2Used = false;
        boolean shard3Used = false;

        for (int i = 0; i < 50; i++) {
            String shardName = selector.getShard(idGenerator.newRecordId());
            assertTrue(shards.containsKey(shardName));

            if (shardName.equals("shard1")) {
                shard1Used = true;
            } else if (shardName.equals("shard2")) {
                shard2Used = true;
            } else if (shardName.equals("shard3")) {
                shard3Used = true;
            }
        }

        assertTrue(shard1Used);
        assertTrue(shard2Used);
        assertTrue(shard3Used);
    }
}
