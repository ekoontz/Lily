              Standalone Lily repository process: how to use
          - o - o - o - o - o - o - o - o - o - o - o - o - o - o -

Prerequisites
=============

You should have HBase running in order to run the Lily repository server.

We assume you know how to obtain and run HBase (and Hadoop).

The HBase and Hadoop versions to be used are defined in the following
properties in Lily's root pom.xml:
 - hbase.version
 - hadoop.version

For HBase, if it would be an SVN-revision based version, you can create the package
equivalent to a binary download version as follows:

svn export -rXXXX http://svn.apache.org/repos/asf/hadoop/hbase/trunk hbase-trunk
cd hbase-trunk
mvn -DskipTests=true package assembly:assembly

You will find the .tar.gz file in the target directory.

Install Kauri
=============

Check the process/server/pom.xml for the Kauri version in use, see the property kauri.version.
Currently this is an SVN snapshot, recognizable by the 'rXXXX' in the version.

You can obtain this Kauri as follows:

svn co -rXXXX https://dev.outerthought.org/svn/outerthought_kauri/trunk kauri-trunk
cd kauri-trunk
mvn -P fast install

After this, there is no package to build or extract, Kauri can be run immediately from its
source tree.

Configure
=========

If your zookeeper quorum does not consist of a single, local-host server
listening on port 2181, you will need to adjust some configuration.

Perform (in the same directory as this README.txt), the following

cp -r conf myconf
cd myconf
find -name .svn rm -rf {} \;

and then edit the files

conf/hbase/hbase.xml
conf/repository/repository.xml

Run
===

From within the same directory as this README.txt, execute:

With standard configuration:
/path/to/kauri-trunk/kauri.sh run

With custom configuration:
/path/to/kauri-trunk/kauri.sh run -c myconf:conf

You can start as many of these processes as you want. By default the server sockets
are bound to ephemeral ports, so there will be no conflicts.

Once you have servers running, you can use the client library to connect to them.
