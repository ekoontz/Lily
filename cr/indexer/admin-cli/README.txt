Indexer administration command line interface
=============================================

Adding an index
---------------

./target/lily-add-index -h

Updating an index
-----------------

./target/lily-update-index -h

Deleting an index
-----------------

This is done by update the index to the state 'DELETE_REQUESTED':

./target/lily-update-index -n indexName --state DELETE_REQUESTED

Rebuilding an index
-------------------

To launch a batch (re)build of an index, use:

./target/lily-update-index -n indexName -b BUILD_REQUESTED

To see whether it started and finished successful, use lily-list-indexes.

Listing the existing indexes
-----------------------------

./target/lily-list-indexes -z localhost:2181

Touching an index
-----------------

Updates the index without modifying it, will cause ZooKeeper watchers to be triggered.

./target/lily-touch-index -z localhost:2181 -n indexName

Downloading the indexer config
------------------------------

./target/lily-get-indexerconf -n indexName -o indexerconf.xml

Downloading the sharding config
-------------------------------

./target/lily-get-shardingconf -n indexName -o shardingconf.json