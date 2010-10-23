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
package org.lilyproject.testfw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Utility to easily launch a full HBase with a temporary storage. Intended to be used to run testcases
 * against (see HBaseProxy connect mode).
 *
 * <p>Disclaimer: much of the code was copied from HBase's HBaseTestingUtility, and slightly adjusted to be able
 * to fix ZK and HDFS port numbers.
 */
public class HBaseRunner {
    private MiniZooKeeperCluster zkCluster = null;
    private MiniDFSCluster dfsCluster = null;
    private MiniHBaseCluster hbaseCluster = null;
    private MiniMRCluster mrCluster = null;
    private File clusterTestBuildDir = null;
    private Configuration conf;
    private int zkPort = 2181;

    public static final String TEST_DIRECTORY_KEY = "test.build.data";
    public static final String DEFAULT_TEST_DIRECTORY = "target/build/data";

    public static void main(String[] args) throws Exception {
        new HBaseRunner().run();
    }

    public void run() throws Exception {
        TestHelper.setupConsoleLogging("INFO");
        TestHelper.setupOtherDefaults();

        conf = HBaseConfiguration.create();

        HBaseProxy.addHBaseTestProps(conf);
        conf.set("hbase.master.info.port", "60010");
        conf.set("hbase.regionserver.info.port", "60030");

        startMiniCluster(1);

        startMiniMapReduceCluster();

        System.out.println("-------------------------");
        System.out.println("Minicluster is up");
        System.out.println();
        System.out.println("To connect to this HBase, use the following properties:");
        System.out.println("hbase.zookeeper.quorum=localhost");
        System.out.println("hbase.zookeeper.property.clientPort=" + zkPort);
        System.out.println();
        System.out.println("In Java code, create the HBase configuration like this:");
        System.out.println("Configuration conf = HBaseConfiguration.create();");
        System.out.println("conf.set(\"hbase.zookeeper.quorum\", \"localhost\");");
        System.out.println("conf.set(\"hbase.zookeeper.property.clientPort\", \"" + zkPort + "\");");
        System.out.println();
        System.out.println("For MapReduce, use:");
        System.out.println("Configuration conf = new Configuration();");
        System.out.println("conf.set(\"mapred.job.tracker\", \"localhost:" + mrCluster.getJobTrackerPort() + "\");");
        System.out.println("Job job = new Job(conf);");
        System.out.println();
        System.out.println("JobTracker web ui:   http://localhost:" + mrCluster.getJobTrackerRunner().getJobTrackerInfoPort());
        System.out.println("HDFS web ui:         http://" + conf.get("dfs.http.address"));
        System.out.println("HBase master web ui: http://localhost:" + hbaseCluster.getMaster().getInfoServer().getPort());
        System.out.println("-------------------------");

        // Seems like Hadoop registers its own shutdown hooks which interfere with ours, so disabled
        // this for now.
//        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
//            public void run() {
//                try {
//                   shutdownMiniCluster();
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                }
//                try {
//                    shutdownMiniMapReduceCluster();
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                }
//            }
//        }));
    }

    public MiniHBaseCluster startMiniCluster(final int servers)
            throws Exception {
        // Make a new random dir to home everything in.  Set it as system property.
        // minidfs reads home from system property.
        this.clusterTestBuildDir = setupClusterTestBuildDir();
        System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.getPath());
        // Bring up mini dfs cluster. This spews a bunch of warnings about missing
        // scheme. Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
        startMiniDFSCluster(servers, this.clusterTestBuildDir);

        // Mangle conf so fs parameter points to minidfs we just started up
        FileSystem fs = this.dfsCluster.getFileSystem();
        this.conf.set("fs.defaultFS", fs.getUri().toString());
        // Do old style too just to be safe.
        this.conf.set("fs.default.name", fs.getUri().toString());
        this.dfsCluster.waitClusterUp();

        // Start up a zk cluster.
        if (this.zkCluster == null) {
            startMiniZKCluster(this.clusterTestBuildDir);
        }

        // Now do the mini hbase cluster.  Set the hbase.rootdir in config.
        Path hbaseRootdir = fs.makeQualified(fs.getHomeDirectory());
        this.conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
        fs.mkdirs(hbaseRootdir);
        FSUtils.setVersion(fs, hbaseRootdir);
        this.hbaseCluster = new MiniHBaseCluster(this.conf, servers);
        // Don't leave here till we've done a successful scan of the .META.
        HTable t = new HTable(this.conf, HConstants.META_TABLE_NAME);
        ResultScanner s = t.getScanner(new Scan());
        while (s.next() != null) continue;

        return this.hbaseCluster;
    }

    private static File setupClusterTestBuildDir() {
        String randomStr = UUID.randomUUID().toString();
        String dirStr = getTestDir(randomStr).toString();
        File dir = new File(dirStr).getAbsoluteFile();
        // Have it cleaned up on exit
        dir.deleteOnExit();
        return dir;
    }

    public static Path getTestDir(final String subdirName) {
        return new Path(getTestDir(), subdirName);
    }

    public static Path getTestDir() {
        return new Path(System.getProperty(TEST_DIRECTORY_KEY, DEFAULT_TEST_DIRECTORY));
    }

    public void shutdownMiniCluster() throws IOException {
        System.out.println("Shutting down minicluster");
        if (this.hbaseCluster != null) {
            this.hbaseCluster.shutdown();
            // Wait till hbase is down before going on to shutdown zk.
            this.hbaseCluster.join();
        }
        shutdownMiniZKCluster();
        if (this.dfsCluster != null) {
            // The below throws an exception per dn, AsynchronousCloseException.
            this.dfsCluster.shutdown();
        }
        // Clean up our directory.
        if (this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
            // Need to use deleteDirectory because File.delete required dir is empty.
            if (!FSUtils.deleteDirectory(FileSystem.getLocal(this.conf),
                    new Path(this.clusterTestBuildDir.toString()))) {
                System.out.println("Failed delete of " + this.clusterTestBuildDir.toString());
            }
            this.clusterTestBuildDir = null;
        }
        System.out.println("Minicluster is down");
    }

    private MiniZooKeeperCluster startMiniZKCluster(final File dir)
            throws Exception {
        if (this.zkCluster != null) {
            throw new IOException("Cluster already running at " + dir);
        }
        this.zkCluster = new MiniZooKeeperCluster();
        zkCluster.setClientPort(zkPort);
        int clientPort = this.zkCluster.startup(dir);
        this.conf.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));
        return this.zkCluster;
    }

    public void shutdownMiniZKCluster() throws IOException {
        if (this.zkCluster != null) {
            this.zkCluster.shutdown();
            this.zkCluster = null;
        }
    }

    public MiniDFSCluster startMiniDFSCluster(int servers, final File dir)
            throws Exception {
        // This does the following to home the minidfscluster
        //     base_dir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
        // Some tests also do this:
        //  System.getProperty("test.cache.data", "build/test/cache");
        if (dir == null) this.clusterTestBuildDir = setupClusterTestBuildDir();
        else this.clusterTestBuildDir = dir;
        System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.toString());
        System.setProperty("test.cache.data", this.clusterTestBuildDir.toString());
        this.dfsCluster = new MiniDFSCluster(9000, this.conf, servers, true, true,
                true, null, null, null, null);
        return this.dfsCluster;
    }

    public void startMiniMapReduceCluster() throws IOException {
        System.out.println("Starting mini mapreduce cluster...");
        // These are needed for the new and improved Map/Reduce framework
        System.setProperty("hadoop.log.dir", conf.get("hadoop.log.dir"));
        conf.set("mapred.output.dir", conf.get("hadoop.tmp.dir"));
        mrCluster = new MiniMRCluster(9001, 0, 1,
                FileSystem.get(conf).getUri().toString(), 1, null, new String[] {"localhost"});
        System.out.println("Mini mapreduce cluster started");
        conf.set("mapred.job.tracker",
                mrCluster.createJobConf().get("mapred.job.tracker"));
    }

    public void shutdownMiniMapReduceCluster() {
        System.out.println("Stopping mini mapreduce cluster...");
        if (mrCluster != null) {
            mrCluster.shutdown();
        }
        // Restore configuration to point to local jobtracker
        conf.set("mapred.job.tracker", "local");
        System.out.println("Mini mapreduce cluster stopped");
    }
}
