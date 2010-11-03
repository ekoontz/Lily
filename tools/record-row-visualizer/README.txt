Tool to visualize the storage structure of record.

Execute ./target/lily-record-row for more info.

To test you can create a record with the import tool:

../../apps/import/target/lily-import sample.json


Ideas for future improvements:
 * make HBase access parameters configurable
 * improve presentation in general
 * improve rendering of various types (blobs, multivalue, etc.)
 * indicate visually what is stored data and what is extra displayed information
   (e.g. field type & record type names)
 * make it possible to view the stored bytes, of cell values but also of
   column names
 * Adjust HBaseTypeManager so that ZooKeeper is not a requirement