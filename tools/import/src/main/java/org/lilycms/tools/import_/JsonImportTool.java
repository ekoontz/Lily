package org.lilycms.tools.import_;

import org.apache.commons.cli.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilycms.client.LilyClient;
import org.lilycms.repository.api.*;
import org.lilycms.util.repo.VersionTag;
import org.lilycms.util.io.Closer;

import static org.lilycms.util.repo.JsonUtil.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class JsonImportTool {
    private ImportTool importTool;
    private Map<String, String> namespaces = new HashMap<String, String>();
    private Repository repository;
    private TypeManager typeManager;

    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";

    public static void main(String[] args) throws Exception {
        Options cliOptions = new Options();

        Option zkOption = OptionBuilder
                .withArgName("quorum")
                .hasArg()
                .withDescription("Zookeeper quorum: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");
        cliOptions.addOption(zkOption);

        Option schemaOnlyOption = OptionBuilder
                .withDescription("Only import the field types and record types, not the records.")
                .withLongOpt("schema-only")
                .create("s");
        cliOptions.addOption(schemaOnlyOption);

        Option helpOption = new Option("h", "help", false, "Shows help");
        cliOptions.addOption(helpOption);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        boolean showHelp = false;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            showHelp = true;
        }

        if (showHelp || cmd.hasOption(helpOption.getOpt())) {
            printHelp(cliOptions);
            System.exit(1);
        }

        if (cmd.getArgList().size() < 1) {
            System.out.println("No import file specified!");
            System.exit(1);
        }

        String zookeeperConnectString;
        if (!cmd.hasOption(zkOption.getOpt())) {
            System.out.println("Zookeeper quorum not specified, using default: " + DEFAULT_ZK_CONNECT);
            zookeeperConnectString = DEFAULT_ZK_CONNECT;
        } else {
            zookeeperConnectString = cmd.getOptionValue(zkOption.getOpt());
        }

        boolean schemaOnly = cmd.hasOption(schemaOnlyOption.getOpt());

        LilyClient client = new LilyClient(zookeeperConnectString);

        for (String arg : (List<String>)cmd.getArgList()) {
            System.out.println("----------------------------------------------------------------------");
            System.out.println("Importing " + arg);
            InputStream is = new FileInputStream(arg);
            try {
                load(client.getRepository(), is, schemaOnly);
            } finally {
                Closer.close(is);
            }
        }
    }

    private static void printHelp(Options cliOptions) {
        HelpFormatter help = new HelpFormatter();
        help.setArgName("[import file]");
        help.printHelp("lily-import", cliOptions, true);
    }

    public static void load(Repository repository, InputStream is, boolean schemaOnly) throws Exception {
        load(repository, new DefaultImportListener(), is, schemaOnly);
    }

    public static void load(Repository repository, ImportListener importListener, InputStream is, boolean schemaOnly)
            throws Exception {
        new JsonImportTool(repository, importListener).load(is, schemaOnly);
    }

    public JsonImportTool(Repository repository, ImportListener importListener) {
        this.importTool = new ImportTool(repository, importListener);
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
    }

    public ImportTool getImportTool() {
        return importTool;
    }

    public void load(InputStream is, boolean schemaOnly) throws Exception {
        // A combination of the Jackson streaming and tree APIs is used: we move streaming through the
        // whole of the file, but use the tree API to load individual items (field types, records, ...).
        // This way things should still work fast and within little memory if anyone would use this to
        // load large amounts of records.

        namespaces.clear();

        JsonFactory jsonFactory = new MappingJsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
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
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        addNamespace(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: namespaces property should be an array. Skipping.");
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

    public void setNamespaces(Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    public void addNamespace(JsonNode node) throws ImportException {
        namespaces.put(getString(node, "prefix"), getString(node, "uri"));
    }

    public FieldType importFieldType(JsonNode node) throws RepositoryException, ImportConflictException, ImportException {
        QName name = getQName(node, "name");

        JsonNode vtype = getNode(node, "valueType");
        String primitive = getString(vtype, "primitive");
        boolean multiValue = getBoolean(vtype, "multiValue", false);
        boolean hierarchical = getBoolean(vtype, "hierarchical", false);

        String scopeName = getString(node, "scope", "non_versioned");
        Scope scope = parseScope(scopeName);

        ValueType valueType = typeManager.getValueType(primitive, multiValue, hierarchical);
        FieldType fieldType = typeManager.newFieldType(valueType, name, scope);

        // Some sanity checks for version tag fields
        if (fieldType.getName().getNamespace().equals(VersionTag.NAMESPACE)) {
            if (fieldType.getScope() != Scope.NON_VERSIONED)
                throw new ImportException("vtag fields should be in the non-versioned scope");

            if (!fieldType.getValueType().getPrimitive().getName().equals("LONG"))
                throw new ImportException("vtag fields should be of type LONG");

            if (fieldType.getValueType().isMultiValue())
                throw new ImportException("vtag fields should not be multi-valued");

            if (fieldType.getValueType().isHierarchical())
                throw new ImportException("vtag fields should not be hierarchical");
        }
        
        return importTool.importFieldType(fieldType);
    }

    public RecordType importRecordType(JsonNode node) throws RepositoryException, ImportException {
        String name = getString(node, "name");
        RecordType recordType = typeManager.newRecordType(name);

        JsonNode fields = getNode(node, "fields");
        for (int j = 0; j < fields.size(); j++) {
            JsonNode field = fields.get(j);
            QName fieldName = getQName(field, "name");
            boolean mandatory = getBoolean(field, "mandatory", false);

            String fieldId;
            try {
                fieldId = typeManager.getFieldTypeByName(fieldName).getId();
            } catch (FieldTypeNotFoundException e) {
                throw new ImportException("Record type " + name + ": field type " + fieldName + " does not exist.");
            }

            recordType.addFieldTypeEntry(fieldId, mandatory);
        }

        return importTool.importRecordType(recordType);
    }

    private void importRecord(JsonNode node) throws RepositoryException, ImportException {
        Record record = repository.newRecord();

        String id = getString(node, "id", null);
        if (id != null) {
            record.setId(repository.getIdGenerator().newRecordId(id));
        }

        String type = getString(node, "type");
        record.setRecordType(type);

        Iterator<String> it = node.getFieldNames();
        while (it.hasNext()) {
            String name = it.next();
            if (name.contains(":")) {
                QName qname = parseQName(name);
                FieldType fieldType = typeManager.getFieldTypeByName(qname);
                Object value = readMultiValue(getNode(node, name), fieldType, name);
                record.setField(qname, value);
            }
        }

        importTool.importRecord(record);
    }

    private Object readMultiValue(JsonNode node, FieldType fieldType, String prop) throws ImportException {
        if (fieldType.getValueType().isMultiValue()) {
            if (!node.isArray()) {
                throw new ImportException("Multi-value value should be specified as array in " + prop);
            }

            List<Object> value = new ArrayList<Object>();
            for (int i = 0; i < node.size(); i++) {
                value.add(readHierarchical(node.get(i), fieldType, prop));
            }

            return value;
        } else {
            return readHierarchical(node, fieldType, prop);
        }
    }

    private Object readHierarchical(JsonNode node, FieldType fieldType, String prop) throws ImportException {
        if (fieldType.getValueType().isHierarchical()) {
            if (!node.isArray()) {
                throw new ImportException("Hierarchical value should be specified as an array in " + prop);
            }

            Object[] elements = new Object[node.size()];
            for (int i = 0; i < node.size(); i++) {
                elements[i] = readPrimitive(node.get(i), fieldType, prop);
            }

            return new HierarchyPath(elements);
        } else {
            return readPrimitive(node, fieldType, prop);
        }
    }

    private Object readPrimitive(JsonNode node, FieldType fieldType, String prop) throws ImportException {
        String primitive = fieldType.getValueType().getPrimitive().getName();

        if (primitive.equals("STRING")) {
            if (!node.isTextual())
                throw new ImportException("Expected text value for " + prop);

            return node.getTextValue();
        } else if (primitive.equals("INTEGER")) {
            if (!node.isIntegralNumber())
                throw new ImportException("Expected int value for " + prop);

            return node.getIntValue();
        } else if (primitive.equals("LONG")) {
            // Special handling for version tag fields
            if (fieldType.getName().getNamespace().equals(VersionTag.NAMESPACE)) {
                if (node.isTextual()) {
                    if (node.getTextValue().equals("last")) {
                        return -1L;
                    } else {
                        throw new ImportException("Version tag fields can only have 'last' as string value. Field " +
                                prop + ", value " + node.getTextValue());
                    }
                } else if (node.isIntegralNumber()) {
                    if (node.getLongValue() < 0) {
                        throw new ImportException("Version tag fields should be larger than 0. Field " + prop +
                                ", value " + node.getLongValue());
                    }
                } else {
                    throw new ImportException("Expected long value or \"last\" for " + prop);
                }
            }

            if (!node.isIntegralNumber())
                throw new ImportException("Expected long value for " + prop);

            return node.getLongValue();
        } else if (primitive.equals("BOOLEAN")) {
            if (!node.isBoolean())
                throw new ImportException("Expected boolean value for " + prop);

            return node.getBooleanValue();
        } else if (primitive.equals("LINK")) {
            if (!node.isTextual())
                throw new ImportException("Expected text value for " + prop);
            
            return Link.fromString(node.getTextValue(), repository.getIdGenerator());
        } else if (primitive.equals("DATE")) {
            if (!node.isTextual())
                throw new ImportException("Expected text value for " + prop);

            return new LocalDate(node.getTextValue());
        } else if (primitive.equals("DATETIME")) {
            if (!node.isTextual())
                throw new ImportException("Expected text value for " + prop);

            return new DateTime(node.getTextValue());
        } else {
            throw new ImportException("Primitive value type not supported by import tool: " + primitive);
        }
    }

    private QName getQName(JsonNode node, String prop) throws ImportException {
        if (node.get(prop) == null) {
            throw new ImportException("Missing required property: " + prop);
        }
        if (!node.get(prop).isTextual()) {
            throw new ImportException("Not a string property: " + prop);
        }
        String name = node.get(prop).getTextValue();
        return parseQName(name);
    }

    private QName parseQName(String name) throws ImportException {
        int pos = name.indexOf(':');
        if (pos == -1) {
            throw new ImportException("Invalid qualified name: " + name);
        }

        String prefix = name.substring(0, pos);
        String localName = name.substring(pos + 1);
        String uri = namespaces.get(prefix);
        if (uri == null) {
            throw new ImportException("Undefined prefix in qualified name: " + name);
        }

        return new QName(uri, localName);
    }

    private Scope parseScope(String scopeName) {
        scopeName = scopeName.toLowerCase();
        if (scopeName.equals("non_versioned")) {
            return Scope.NON_VERSIONED;
        } else if (scopeName.equals("versioned")) {
            return Scope.VERSIONED;
        } else if (scopeName.equals("versioned-mutable")) {
            return Scope.VERSIONED_MUTABLE;
        } else {
            throw new RuntimeException("Unrecognized scope name: " + scopeName);
        }
    }
}
