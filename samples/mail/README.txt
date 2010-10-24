mail sample
===========

Lily includes a tool to import mbox mail archives, lily-mbox-import.

The remainder of this file describes how to get started using it.

Get some mbox files
-------------------

One source of mbox files are the Apache mailing list archives, which can be found at:

http://{top level project}.apache.org/mail/{list name}

Here is how you can easily download them:

curl -f http://hadoop.apache.org/mail/mapreduce-user/[2008-2010][01-12].gz -o "#1#2.gz"

curl -f http://cocoon.apache.org/mail/dev/[2000-2010][01-12].gz -o "#1#2.gz"

Other mbox sources:
 * Gmane allows getting mbox archives (but warns about overusing the service):
   http://gmane.org/export.php
 * Linux kernel list archives:
   http://userweb.kernel.org/~akpm/lkml-mbox-archives/

Run HBase & Lily
----------------

Obviously you need to have HBase and Lily running.

To run a test HBase instance, use:

launch-hadoop

Start the Lily server using:

lily-server

Create the schema
-----------------

If you run the import tool with the -s option, it will just create the schema.

lily-mbox-import -s

If you need to connect to a ZooKeeper different from 'localhost:2181', use the
-z option to specify the connection string.

Run SOLR and define an index
----------------------------

This step is optional and can be skipped.

A sample SOLR schema configuration is provided in the file mail_solr_schema.xml

To run a test SOLR instance with this configuration, use:

solr-launcher -s mail_solr_schema.xml

Now configure an index in SOLR using:

lily-add-index -n mail -s shard1:http://localhost:8983/solr/ -c mail_indexerconf.xml

Run the import
--------------

You can import one file at a time or a complete directory. Files ending in ".gz"
will be decompressed on the fly.

lily-mbox-import -f {file name or directory name}

Again, use -z to specify the ZooKeeper connection string:
lily-mbox-import -z localhost:2181 -f {file name or directory name}