package org.lilycms.tools.import_.cli;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.core.*;
import org.lilycms.tools.import_.json.*;
import org.lilycms.util.json.JsonFormat;

import java.io.InputStream;

public class JsonImport {
    private Namespaces namespaces = new Namespaces();
    private Repository repository;
    private TypeManager typeManager;
    private ImportListener importListener;

    public static void load(Repository repository, InputStream is, boolean schemaOnly) throws Exception {
        load(repository, new DefaultImportListener(), is, schemaOnly);
    }

    public static void load(Repository repository, ImportListener importListener, InputStream is, boolean schemaOnly)
            throws Exception {
        new JsonImport(repository, importListener).load(is, schemaOnly);
    }

    public JsonImport(Repository repository, ImportListener importListener) {
        this.importListener = importListener;
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
    }

    public void load(InputStream is, boolean schemaOnly) throws Exception {
        // A combination of the Jackson streaming and tree APIs is used: we move streaming through the
        // whole of the file, but use the tree API to load individual items (field types, records, ...).
        // This way things should still work fast and within little memory if anyone would use this to
        // load large amounts of records.

        namespaces.clear();

        MappingJsonFactory jsonFactory = new MappingJsonFactory();
        JsonFormat.setNonStdFeatures(jsonFactory);
        JsonParser jp = jsonFactory.createJsonParser(is);

        JsonToken current;
        current = jp.nextToken();

        if (current != JsonToken.START_OBJECT) {
            System.out.println("Error: expected object node as root of the input. Giving up.");
            return;
        }

        while (jp.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            current = jp.nextToken(); // move from field name to field value
            if (fieldName.equals("namespaces")) {
                if (current == JsonToken.START_OBJECT) {
                    readNamespaces((ObjectNode)jp.readValueAsTree());
                } else {
                    System.out.println("Error: namespaces property should be an object. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("fieldTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importFieldType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: fieldTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("recordTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importRecordType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: recordTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("records")) {
                if (!schemaOnly) {
                    if (current == JsonToken.START_ARRAY) {
                        while (jp.nextToken() != JsonToken.END_ARRAY) {
                            importRecord(jp.readValueAsTree());
                        }
                    } else {
                        System.out.println("Error: records property should be an array. Skipping.");
                        jp.skipChildren();
                    }
                } else {
                    jp.skipChildren();
                }
            }
        }
    }

    public void readNamespaces(ObjectNode node) throws JsonFormatException {
        this.namespaces = NamespacesConverter.fromJson(node);
    }

    public Namespaces getNamespaces() {
        return namespaces;
    }

    public FieldType importFieldType(JsonNode node) throws RepositoryException, ImportConflictException,
            ImportException, JsonFormatException {

        if (!node.isObject()) {
            throw new ImportException("Field type should be specified as object node.");
        }

        FieldType fieldType = FieldTypeReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);

        if (fieldType.getName() == null) {
            throw new ImportException("Missing name property on field type.");
        }

        ImportResult<FieldType> result = FieldTypeImport.importFieldType(fieldType, ImportMode.CREATE_OR_UPDATE,
                IdentificationMode.NAME, fieldType.getName(), typeManager);
        fieldType = result.getEntity();

        switch (result.getResultType()) {
            case CREATED:
                importListener.created(EntityType.FIELD_TYPE, fieldType.getName().toString(), fieldType.getId());
                break;
            case UP_TO_DATE:
                importListener.existsAndEqual(EntityType.FIELD_TYPE, fieldType.getName().toString(), null);
                break;
            case CONFLICT:
                importListener.conflict(EntityType.FIELD_TYPE, fieldType.getName().toString(),
                        result.getConflictingProperty(), result.getConflictingOldValue(),
                        result.getConflictingNewValue());
                break;
            default:
                throw new ImportException("Unexpected import result type for field type: " + result.getResultType());
        }

        return fieldType;
    }

    public RecordType importRecordType(JsonNode node) throws RepositoryException, ImportException, JsonFormatException {

        if (!node.isObject()) {
            throw new ImportException("Record type should be specified as object node.");
        }

        RecordType recordType = RecordTypeReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);
        return importRecordType(recordType);
    }

    public RecordType importRecordType(RecordType recordType) throws RepositoryException, ImportException, JsonFormatException {

        if (recordType.getName() == null) {
            throw new ImportException("Missing name property on record type.");
        }

        ImportResult<RecordType> result = RecordTypeImport.importRecordType(recordType, ImportMode.CREATE_OR_UPDATE,
                IdentificationMode.NAME, recordType.getName(), typeManager);
        recordType = result.getEntity();

        switch (result.getResultType()) {
            case CREATED:
                importListener.created(EntityType.RECORD_TYPE, recordType.getName().toString(), recordType.getId());
                break;
            case UPDATED:
                importListener.updated(EntityType.RECORD_TYPE, null, recordType.getId(), recordType.getVersion());
                break;
            case UP_TO_DATE:
                importListener.existsAndEqual(EntityType.RECORD_TYPE, recordType.getName().toString(), null);
                break;
            default:
                throw new ImportException("Unexpected import result type for record type: " + result.getResultType());
        }

        return recordType;
    }

    private Record importRecord(JsonNode node) throws RepositoryException, ImportException, JsonFormatException {

        if (!node.isObject()) {
            throw new ImportException("Record should be specified as object node.");
        }

        Record record = RecordReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);

        ImportResult<Record> result = RecordImport.importRecord(record, ImportMode.CREATE_OR_UPDATE, repository);
        record = result.getEntity();

        switch (result.getResultType()) {
            case CREATED:
                importListener.created(EntityType.RECORD, null, record.getId().toString());
                break;
            case UP_TO_DATE:
                importListener.existsAndEqual(EntityType.RECORD, null, record.getId().toString());
                break;
            case UPDATED:
                importListener.updated(EntityType.RECORD, null, record.getId().toString(), record.getVersion());
                break;
            default:
                throw new ImportException("Unexpected import result type for record: " + result.getResultType());
        }

        return record;
    }
}
