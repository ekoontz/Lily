                     Welcome to the Lily Content Repository
                     --------------------------------------
                          http://www.lilyproject.org/


Getting started
===============

For a first introduction to using Lily, please see:

http://docs.outerthought.org/lily-docs-trunk/414-lily.html

Prerequisites
=============

Install Maven 2.2.x
-------------------

From http://maven.apache.org

Windows users
-------------

Install cygwin, and add its bin directory to the PATH.

You do not need to run any commands via the cygwin shell (use the provided
bat files instead), it is Hadoop which executes some unix commands like ls.

Building Lily
=============

Execute

mvn install

or to run it faster (without the tests):

mvn -Pfast install

On running test
===============

Log output of testcases is by default sent to a target/log.txt, errors
are however always logged to the console. Debug output to the console of
selected log categories can be enabled by running tests as follows:

mvn -Plog test

This is mostly useful when working on individual subprojects/tests.

Running tests (faster) against a stand-alone HBase
--------------------------------------------------

Test run rather slow because HBase-based tests launch a mini
Hadoop/ZooKeeper/HBase-cluster as part of the testcase. While this takes
some time in itself, it is especially the creation of tables in HBase which
takes time.

The tests can be sped up by starting an independent cluster and running the
tests against that. Instead of dropping and recreating tables between each
test, the tables are emptied by deleting all rows from them (thus be very
careful against which HBase you run this!). Besides the speed advantages,
this is also easier for debugging.


                     === Quick way: dummy HBase ===


To easily launch a mini HBase without having to install it, execute:

cd testfw
./target/launch-hadoop

This prints a line "Minicluster is up" when it is started, though it is
quickly followed by more logging.

And then run the tests with

mvn -Pconnect test

The first time this will still take more time (though already quite a bit
less than before), since the tables still need to be created. Subsequent
runs should be way faster.

Each time this 'mini' HBase is restarted, it looses its state so the first run
after restart will again be a bit slower.

When you run testcases without -Pconnect, and you have a mini HBase launched,
it might get confused (because some of the same ports are used), and you
might need to restart it.


                 === Run against existing cluster ===


If you want to connect to an HBase you have installed, you need to specify
the name(s) and port of Zookeeper:

mvn -Pconnect -DargLine="-Dlily.test.hbase.zookeeper.quorum=localhost -Dlily.test.hbase.zookeeper.property.clientPort=2181 -Dlily.test.dfs=hdfs://localhost:9000" test

The property lily.test.dfs points to the HDFS to be used to store blobs,
the value shown here is the default.

Any HBase property can be specified by prefixing it with
"lily.test." (also when the tests run with an embedded HBase).

Combining profiles
==================

Maven profiles can be combined, for example:

mvn -Pconnect -Plog test

or, if you prefer, like this:

mvn -Pconnect,log test
