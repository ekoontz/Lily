Tester
======

This is a simple load testing utility that performs CRUD operations in a loop.

All configuration is specified in a json file, see config.json, which is specified
as argument to Tester.

Tester keeps all created records in memory, in order to be able to later check
the result of read operations, or to be able to update existing records.

Tester runs only one thread. To do multi-threaded testing, run multiple instances.

Logging is performed to two files: one containing a line per CRUD operation, with its
duration and success tate. Another file contains the stacktraces of errors that occured.

In a source setting, you can run this tool with maven:

./target/lily-tester config.json
