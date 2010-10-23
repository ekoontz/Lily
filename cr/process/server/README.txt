              Standalone Lily repository process: how to use
              ----------------------------------------------

Running HBase
=============

You should have HBase running in order to run the Lily repository server.

You can either:

 * launch a dummy Hadoop/HBase/ZooKeeper via:
      cd testfw
      ./target/launch-hadoop

 * or use a HBase installation you set up yourself.
   See Lily's root pom.xml for the version we build against, properties:
     - hbase.version
     - hadoop.version

Configure
=========

If your zookeeper quorum does not consist of a single, local-host server
listening on port 2181, you will need to adjust some configuration.

Perform (in the same directory as this README.txt), the following

cp -r conf myconf
cd myconf
find -name .svn rm -rf {} \;

and then edit the files

conf/general/hbase.xml
conf/general/zookeeper.xml

Run
===

From within the same directory as this README.txt, execute:

With standard configuration:
./target/lily-server

With custom configuration:
./target/lily-server -c myconf:conf

You can start as many of these processes as you want. By default the server sockets
are bound to ephemeral ports, so there will be no conflicts.

Once you have servers running, you can use the client library to connect to them.

Running via normal Kauri script
===============================

Note that instead of launching the lily-server script, you can as well simply
launch Kauri if you have it installed, for example:

/path/to/kauri-trunk/kauri.sh run -c myconf:conf

You can obtain Kauri as follows:

svn co https://dev.outerthought.org/svn/outerthought_kauri/trunk kauri-trunk
cd kauri-trunk
mvn -P fast install
