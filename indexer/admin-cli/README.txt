Indexer administration command line interface
=============================================

Adding an index
---------------

mvn -o exec:java \
-Dexec.mainClass=org.lilycms.indexer.admin.cli.AddIndexCli \
-Dexec.args="-h"

Updating an index
-----------------

mvn -o exec:java \
-Dexec.mainClass=org.lilycms.indexer.admin.cli.UpdateIndexCli \
-Dexec.args="-h"

Listing the existing indexes
-----------------------------

mvn -o exec:java \
-Dexec.mainClass=org.lilycms.indexer.admin.cli.ListIndexesCli \
-Dexec.args="-z localhost:2181"

Touching an index
-----------------

Updates the index without modifying it, will cause ZooKeeper watchers to be triggered.

mvn -o exec:java \
-Dexec.mainClass=org.lilycms.indexer.admin.cli.TouchIndexCli \
-Dexec.args="-z localhost:2181 -n indexName"
