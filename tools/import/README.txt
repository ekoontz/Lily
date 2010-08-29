Import
======

This is a tool that allows importing field types, record types and records
into a Lily repository.

For field types and record types, it checks if they already exist, if so
compares if they are the same, if not updates them if possible or otherwise
gives an error.

TODO: describe the json format

In a source setting, you can run this tool with maven:

mvn exec:java -Dexec.args="-z localhost:2181 sample.json"
