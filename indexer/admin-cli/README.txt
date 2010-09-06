Indexer administration command line interface
=============================================

Adding an index
---------------

./target/lily-add-index -h

Updating an index
-----------------

./target/lily-update-index -h

Listing the existing indexes
-----------------------------

./target/lily-list-indexes -z localhost:2181

Touching an index
-----------------

Updates the index without modifying it, will cause ZooKeeper watchers to be triggered.

./target/lily-touch-index -z localhost:2181 -n indexName