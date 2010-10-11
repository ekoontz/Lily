package org.lilycms.indexer.master;

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
import org.lilycms.indexer.fullbuild.IndexingMapper;
import org.lilycms.indexer.model.api.IndexDefinition;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Map;

public class FullIndexBuilder {
    private static final byte[] DELETED_COLUMN_NAME = Bytes.toBytes("$Deleted");
    private static final byte[] NON_VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("NVSCF");

    /**
     *
     * @return the ID of the started job
     */
    public static String startBatchBuildJob(IndexDefinition index, Configuration mapReduceConf,
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
        job.getConfiguration().set("org.lilycms.indexer.fullbuild.indexerconf", indexerConfString);

        if (index.getShardingConfiguration() != null) {
            String shardingConfString = Base64.encodeBytes(index.getShardingConfiguration(), Base64.GZIP);
            job.getConfiguration().set("org.lilycms.indexer.fullbuild.shardingconf", shardingConfString);
        }

        int i = 0;
        for (Map.Entry<String, String> shard : index.getSolrShards().entrySet()) {
            i++;
            job.getConfiguration().set("org.lilycms.indexer.fullbuild.solrshard.name." + i, shard.getKey());
            job.getConfiguration().set("org.lilycms.indexer.fullbuild.solrshard.address." + i, shard.getValue());
        }

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        //
        // Define the HBase scanner
        //
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new SingleColumnValueFilter(NON_VERSIONED_SYSTEM_COLUMN_FAMILY,
                DELETED_COLUMN_NAME, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true)));
        Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.addColumn(NON_VERSIONED_SYSTEM_COLUMN_FAMILY, DELETED_COLUMN_NAME);

        TableMapReduceUtil.initTableMapperJob("recordTable", scan,
            IndexingMapper.class, null, null, job);

        //
        // Provide properties to connect to HBase
        //
        job.getConfiguration().set("hbase.zookeeper.quorum", hbaseConf.get("hbase.zookeeper.quorum"));
        job.getConfiguration().set("hbase.zookeeper.property.clientPort", hbaseConf.get("hbase.zookeeper.property.clientPort"));

        //
        // Provide Lily ZooKeeper props
        //
        job.getConfiguration().set("org.lilycms.indexer.fullbuild.zooKeeperConnectString", zkConnectString);
        job.getConfiguration().set("org.lilycms.indexer.fullbuild.zooKeeperSessionTimeout", String.valueOf(zkSessionTimeout));

        job.submit();
        return job.getJobID().toString();
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
