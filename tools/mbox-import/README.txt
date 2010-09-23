mbox import tool
================

With this tool you can import mbox mail archives into Lily.

Usage
=====

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

Create the schema
-----------------

If you run the import tool without any options, it will just create the schema.

cd tools/mbox-import
./target/lily-mbox-import

If you need to connect to a ZooKeeper different from 'localhost:2181', use the
-z option to specify the connection string.

Configure SOLR
---------------

cd tools/mbox-import
cp mail_solr_schema.xml {solr}/example/solr/conf/schema.xml

Define an index
---------------

cd indexer/admin-cli
./target/lily-add-index -n mail -s shard1:http://localhost:8983/solr/ -c ../../tools/mbox-import/mail_indexerconf.xml

Run the import
--------------

You can import one file at a time or a complete directory. Files ending in ".gz"
will be decompressed on the fly.

cd tools/mbox-import
./target/lily-mbox-import -f {file name or directory name}

Again, use -z to specify the ZooKeeper connection string:
./target/lily-mbox-import -z localhost:2181 -f {file name or directory name}


FUTURE IDEAS
============

 * while it was not the intention to create a real mail archive, maybe with some
   more effort we can make it better suited to be one: especially store the main
   mail text with the message itself, and the remaining parts as 'attachment' records.
   (see also http://en.wikipedia.org/wiki/MIME#Multipart_subtypes)

 * when importing a directory of files, launch a number of threads to import files in parallel

 * error handling: handle situations like bad input files or lost lily/zookeeper connections.
   Maybe upon exit (in any situation except kill -9) try to write a file with how far we
   got in the import (files + number of the message within the file), and allow to resume
   from there upon next start.

 * improve the indexer/solr configuration, e.g. with some fields suited for faceted queries
 
