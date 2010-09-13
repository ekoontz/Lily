mbox import tool
================

With this tool you can import mbox mail archives into Lily.

Some sources of mbox files:

http://mail-archives.apache.org/

or via:

http://{top level project}.apache.org/mail/ for example http://hadoop.apache.org/mail/


Usage
=====

Set SOLR config:

cd tools/mbox-import
cp mail_solr_schema.xml {solr}/example/solr/conf/schema.xml

Add the indexer conf:

cd indexer/admin-cli
./target/lily-add-index -n mail -s http://localhost:8983/solr/ -c ../../tools/mbox-import/mbox_indexerconf.xml

Run:

cd tools/mbox-import
./target/lily-mbox-import {name of mbox file}