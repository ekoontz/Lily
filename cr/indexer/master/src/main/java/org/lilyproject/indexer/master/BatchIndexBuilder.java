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
package org.lilyproject.indexer.master;

import net.iharder.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.lilyproject.indexer.batchbuild.IndexingMapper;
import org.lilyproject.indexer.model.api.IndexDefinition;
import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Map;

public class BatchIndexBuilder {
    /**
     *
     * @return the ID of the started job
     */
    public static Job startBatchBuildJob(IndexDefinition index, Configuration mapReduceConf,
            Configuration hbaseConf, String zkConnectString, int zkSessionTimeout) throws Exception {

        Configuration conf = new Configuration(mapReduceConf);
        Job job = new Job(conf);

        //
        // Find and set the MapReduce job jar.
        //
        Class mapperClass = IndexingMapper.class;
        String jobJar = findContainingJar(mapperClass);
        if (jobJar == null) {
            // TODO
            throw new RuntimeException("Job jar not found for class " + mapperClass);
        }

        job.getConfiguration().set("mapred.jar", jobJar);

        //
        // Pass information about the index to be built
        //
        String indexerConfString = Base64.encodeBytes(index.getConfiguration(), Base64.GZIP);
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.indexerconf", indexerConfString);

        if (index.getShardingConfiguration() != null) {
            String shardingConfString = Base64.encodeBytes(index.getShardingConfiguration(), Base64.GZIP);
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.shardingconf", shardingConfString);
        }

        int i = 0;
        for (Map.Entry<String, String> shard : index.getSolrShards().entrySet()) {
            i++;
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.solrshard.name." + i, shard.getKey());
            job.getConfiguration().set("org.lilyproject.indexer.batchbuild.solrshard.address." + i, shard.getValue());
        }

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        //
        // Define the HBase scanner
        //
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new SingleColumnValueFilter(RecordCf.SYSTEM.bytes,
                RecordColumn.DELETED.bytes, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true)));
        Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.addColumn(RecordCf.SYSTEM.bytes, RecordColumn.DELETED.bytes);

        TableMapReduceUtil.initTableMapperJob(Table.RECORD.name, scan,
            IndexingMapper.class, null, null, job);

        //
        // Provide properties to connect to HBase
        //
        job.getConfiguration().set("hbase.zookeeper.quorum", hbaseConf.get("hbase.zookeeper.quorum"));
        job.getConfiguration().set("hbase.zookeeper.property.clientPort", hbaseConf.get("hbase.zookeeper.property.clientPort"));

        //
        // Provide Lily ZooKeeper props
        //
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.zooKeeperConnectString", zkConnectString);
        job.getConfiguration().set("org.lilyproject.indexer.batchbuild.zooKeeperSessionTimeout", String.valueOf(zkSessionTimeout));

        job.submit();

        return job;
    }

    /**
     * This method was copied from Hadoop JobConf (Apache License).
     */
    private static String findContainingJar(Class my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for(Enumeration itr = loader.getResources(class_file);
                itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
