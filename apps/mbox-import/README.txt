mbox import tool
================

With this tool you can import mbox mail archives into Lily.

For more explanation on how to use it, see {source tree root}/samples/mail.

In a source setting, you can run this tool as follows:

./target/lily-mbox-import

Specify the -h option for more help.






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

