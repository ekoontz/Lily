package org.lilycms.indexer.master;

import net.iharder.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
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
import java.util.List;

public class FullIndexBuilder {
    /**
     *
     * @return the ID of the started job
     */
    public static String startRebuildJob(IndexDefinition index, Configuration mapReduceConf) throws Exception {
        Configuration conf = new Configuration(mapReduceConf);
        Job job = new Job(conf);

        Class mapperClass = IndexingMapper.class;
        String jobJar = findContainingJar(mapperClass);
        if (jobJar == null) {
            // TODO
            throw new RuntimeException("Job jar not found for class " + mapperClass);
        }

        job.getConfiguration().set("mapred.jar", jobJar);

        String indexerConfString = Base64.encodeBytes(index.getConfiguration(), Base64.GZIP);
        job.getConfiguration().set("org.lilycms.indexer.fullbuild.indexerconf", indexerConfString);

        List<String> solrShards = index.getSolrShards();
        for (int i = 0; i < solrShards.size(); i++) {
            job.getConfiguration().set("org.lilycms.indexer.fullbuild.solrshard." + (i + 1), solrShards.get(i));
        }

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        // TODO the scan should skip deleted records.
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("NVSCF"), Bytes.toBytes("$NonVersionableRecordTypeId"));

        TableMapReduceUtil.initTableMapperJob("recordTable", scan,
            IndexingMapper.class, null, null, job);

        // TODO get these from somewhere
        job.getConfiguration().set("hbase.zookeeper.quorum", "localhost");
        job.getConfiguration().set("hbase.zookeeper.property.clientPort", "2181");

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
