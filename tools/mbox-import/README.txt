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
./target/lily-add-index -n mail -s http://localhost:8983/solr/ -c ../../tools/mbox-import/mail_indexerconf.xml

Run the import
--------------

You can import one file at a time or a complete directory. Files ending in ".gz"
will be decompressed on the fly.

cd tools/mbox-import
./target/lily-mbox-import -f {file name or directory name}

Again, use -z to specify the ZooKeeper connection string:
./target/lily-mbox-import -z localhost:2181 -f {file name or directory name}
