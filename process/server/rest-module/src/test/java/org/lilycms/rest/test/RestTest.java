package org.lilycms.rest.test;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.lilycms.testfw.HBaseProxy;
import org.lilycms.util.io.Closer;
import org.lilycms.util.repo.JsonUtil;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;

import java.io.IOException;
import java.io.InputStream;

public class RestTest {
    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();
    private final static KauriTestUtility KAURI_TEST_UTIL = new KauriTestUtility();
    private static String BASE_URI;

    private static Client CLIENT;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        HBASE_PROXY.start();

        KAURI_TEST_UTIL.createDefaultConf(HBASE_PROXY);
        KAURI_TEST_UTIL.start();

        CLIENT = KAURI_TEST_UTIL.getClient();

        BASE_URI = "http://localhost:" + KAURI_TEST_UTIL.getPort();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        KAURI_TEST_UTIL.stop();
        HBASE_PROXY.stop();
    }

    @Test
    public void testSchema() throws Exception {
        // Create field type using POST
        String body = json("{action: 'create', fieldType: {name: 'n$field1', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create field type using PUT
        body = json("{name: 'n$field2', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilycms.resttest", body);
        assertStatus(Status.SUCCESS_CREATED, response);
    }

    @Test
    public void test() throws Exception {
        // Create field type
        String body = json("{action: 'create', fieldType: {name: 'b$title', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);

        assertStatus(Status.SUCCESS_CREATED, response);

        JsonNode jsonNode = readJson(response.getEntity());
        String prefix = jsonNode.get("namespaces").get("org.lilycms.resttest").getTextValue();
        assertEquals(prefix + "$title", jsonNode.get("name").getTextValue());
        String titleFieldId = jsonNode.get("id").getTextValue();
        assertEquals(BASE_URI + "/schema/fieldTypeById/" + titleFieldId, response.getLocationRef().toString());

        // Read the field type
        response = get(response.getLocationRef().toString());
        assertStatus(Status.SUCCESS_OK, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'b$book', fields: [ {name: 'b$title'} ]," +
                "namespaces: { 'org.lilycms.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);

        assertStatus(Status.SUCCESS_CREATED, response);

        jsonNode = readJson(response.getEntity());
        prefix = jsonNode.get("namespaces").get("org.lilycms.resttest").getTextValue();
        assertEquals(prefix + "$book", jsonNode.get("name").getTextValue());

        // Read the record type
        response = get(response.getLocationRef().toString());
        assertStatus(Status.SUCCESS_OK, response);

        // Create a record
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.faster_fishing", body);

        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.SUCCESS_OK, response);

        // Read the record as specific version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/1");
        assertStatus(Status.SUCCESS_OK, response);

        // Read a non-existing version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/10");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Specify non-numeric version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/abc");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Update the record
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new)' }, namespaces : { 'org.lilycms.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(Status.SUCCESS_OK, response);
        // TODO verify version was updated

        // Update a record which does not exist, should fail
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new)' }, namespaces : { 'org.lilycms.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.non_existing_record", body);
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Update the field type name
        body = json("{name: 'b$title2', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + titleFieldId, body);

        assertStatus(Status.SUCCESS_OK, response);

        response = get(BASE_URI + "/schema/fieldType/b$title2?ns.b=org.lilycms.resttest");
        assertStatus(Status.SUCCESS_OK, response);

        // Delete the record
        response = delete(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.SUCCESS_OK, response);

        // Verify deleted record is gone
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Verify delete is idempotent
        response = delete(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.SUCCESS_OK, response);
    }

    @Test
    public void testBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob1', valueType: { primitive: 'BLOB' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT', fields: [ {name: 'b$blob1'} ]," +
                "namespaces: { 'org.lilycms.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Upload a blob
        Request req = new Request(Method.POST, BASE_URI + "/blob");
        String data = "Hello, blob world!";
        Representation blobRepr = new StringRepresentation(data, MediaType.TEXT_PLAIN);
        req.setEntity(blobRepr);
        response = CLIENT.handle(req);

        assertStatus(Status.SUCCESS_CREATED, response);
        JsonNode jsonNode = readJson(response.getEntity());
        byte[] blobValue = jsonNode.get("value").getBinaryValue();
        assertEquals("text/plain", jsonNode.get("mimeType").getTextValue());
        assertEquals((long)data.length(), jsonNode.get("size").getLongValue());

        // Create a record with this blob
        ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
        recordNode.put("type", "b$blobRT");
        ObjectNode fieldsNode = recordNode.putObject("fields");
        ObjectNode blobNode = fieldsNode.putObject("b$blob1");
        blobNode.put("size", data.length());
        blobNode.put("mimeType", "text/plain");
        blobNode.put("value", blobValue);
        ObjectNode nsNode = recordNode.putObject("namespaces");
        nsNode.put("org.lilycms.resttest", "b");

        response = put(BASE_URI + "/record/USER.blob1", recordNode.toString());
        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.blob1");
        assertStatus(Status.SUCCESS_OK, response);

        jsonNode = readJson(response.getEntity());
        // TODO verify again

        // Read the blob
        response = get(BASE_URI + "/record/USER.blob1/field/b$blob1/data?ns.b=org.lilycms.resttest");
        assertStatus(Status.SUCCESS_OK, response);
        // TODO verify content equals what was uploaded
    }

    @Test
    public void testVersionCollection() throws Exception {
        // Create some field types
        String body = json("{action: 'create', fieldType: {name: 'p$name', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'p' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$price', valueType: { primitive: 'DOUBLE' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$colour', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'p$product', fields: [ {name: 'p$name'}, " +
                "{name: 'p$price'}, {name: 'p$colour'} ], namespaces: { 'org.lilycms.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record with some versions
        body = json("{ type: 'p$product', fields: { 'p$name' : 'Product 1' }, namespaces : { 'org.lilycms.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5 }, namespaces : { 'org.lilycms.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(Status.SUCCESS_OK, response);

        // Get list of versions
        response = get(BASE_URI + "/record/USER.product1/version"); //?ns.b=org.lilycms.resttest");
        System.out.println(response.getEntityAsText());
        assertStatus(Status.SUCCESS_OK, response);
        // TODO test response and more options (field list, range)
    }

    @Test
    public void testVariantCollection() throws Exception {
        String body = json("{ type: 'b$book', fields: { 'b$title2' : 'Hunting' }, namespaces : { 'org.lilycms.resttest': 'b' } }");
        Response response = put(BASE_URI + "/record/USER.hunting.lang=en", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title2' : 'Jagen' }, namespaces : { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.hunting.lang=nl", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title2' : 'La chasse' }, namespaces : { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.hunting.lang=fr", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        response = get(BASE_URI + "/record/USER.hunting/variant");
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode jsonNode = readJson(response.getEntity());
        ArrayNode resultsNode = (ArrayNode)jsonNode.get("results");

        assertEquals(3, resultsNode.size());

        // Keys are returned in storage order (though this is more of an implementation detail on which clients should not rely)
        assertEquals("USER.hunting.lang=en", resultsNode.get(0).get("id").getTextValue());
        assertEquals("USER.hunting.lang=fr", resultsNode.get(1).get("id").getTextValue());
        assertEquals("USER.hunting.lang=nl", resultsNode.get(2).get("id").getTextValue());
    }

    @Test
    public void testRecordByVTag() throws Exception {
        // Create 'active' vtag field
        String body = json("{action: 'create', fieldType: {name: 'v$active', valueType: { primitive: 'LONG' }, " +
                "scope: 'non_versioned', namespaces: { 'org.lilycms.vtag': 'v' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create 'last' vtag field
        body = json("{action: 'create', fieldType: {name: 'v$last', valueType: { primitive: 'LONG' }, " +
                "scope: 'non_versioned', namespaces: { 'org.lilycms.vtag': 'v' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title2' : 'Title version 1' }, namespaces : { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.vtagtest", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title2' : 'Title version 2', 'v$active': 1, 'v$last': 2 }, " +
                "namespaces : { 'org.lilycms.resttest': 'b', 'org.lilycms.vtag': 'v' } }");
        response = put(BASE_URI + "/record/USER.vtagtest", body);
        assertStatus(Status.SUCCESS_OK, response);

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/active");
        assertStatus(Status.SUCCESS_OK, response);
        assertEquals(1L, readJson(response.getEntity()).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/last");
        assertStatus(Status.SUCCESS_OK, response);
        assertEquals(2L, readJson(response.getEntity()).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/aNotExistingVTagName");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);
    }

    private void assertStatus(Status expectedStatus, Response response) throws IOException {
        if (!expectedStatus.equals(response.getStatus())) {
            System.err.println("Detected unexpected response status, body of the response is:");
            printErrorResponse(response);
            assertEquals(expectedStatus, response.getStatus());
        }
    }

    private void printErrorResponse(Response response) throws IOException {
        if (response.getEntity().getMediaType().equals(MediaType.APPLICATION_JSON)) {
            JsonNode json = readJson(response.getEntity());
            System.err.println("Error:");
            System.err.println("  Description: " + json.get("description").getTextValue());
            System.err.println("  Status: " + json.get("status").getIntValue());
            if (json.get("causes") != null) {
                System.err.println("  Causes:");
                ArrayNode causes = (ArrayNode)json.get("causes");
                for (int i = 0; i < causes.size(); i++) {
                    ObjectNode causeNode = (ObjectNode)causes.get(i);
                    System.err.println("    Cause message: " + causeNode.get("message").getTextValue());
                    System.err.println("    Cause type: " + causeNode.get("type").getTextValue());
                    System.err.println("    StackTrace:");
                    ArrayNode stNode = (ArrayNode)causeNode.get("stackTrace");
                    for (int j = 0; j < stNode.size(); j++) {
                        ObjectNode steNode = (ObjectNode)stNode.get(j);
                        String className = steNode.get("class").getTextValue();
                        String method = steNode.get("method").getTextValue();
                        String file = steNode.get("file") != null ? steNode.get("file").getTextValue() : null;
                        int line = steNode.get("line").getIntValue();
                        boolean nativeMethod = steNode.get("native") != null && steNode.get("native").getBooleanValue();

                        System.err.println("      " + className + "." + method +
                                (nativeMethod ? "(Native Method)" :
                                        (file != null && line >= 0 ?
                                                "(" + file + ":" + line + ")" :
                                                (file != null ? "(" + file + ")" : "(Unknown Source)"))));
                    }

                    int common = JsonUtil.getInt(causeNode, "stackTraceCommon", -1);
                    if (common != -1) {
                        System.err.println("      " + common + " more");
                    }
                }
            }
        } else {
            System.err.println(response.getEntityAsText());
        }
    }
    
    private Response post(String uri, String body) {
        Request req = new Request(Method.POST, uri);
        req.setEntity(body, MediaType.APPLICATION_JSON);
        return CLIENT.handle(req);
    }

    private Response put(String uri, String body) {
        Request req = new Request(Method.PUT, uri);
        req.setEntity(body, MediaType.APPLICATION_JSON);
        return CLIENT.handle(req);
    }

    private Response get(String uri) {
        return CLIENT.handle(new Request(Method.GET, uri));
    }

    private Response delete(String uri) {
        return CLIENT.handle(new Request(Method.DELETE, uri));
    }

    public static JsonNode readJson(Representation representation) throws IOException {
        JsonFactory jsonFactory = new MappingJsonFactory();
        InputStream is = representation.getStream();
        try {
            JsonParser jp = jsonFactory.createJsonParser(is);
            return jp.readValueAsTree();
        } finally {
            Closer.close(is);
        }
    }

    private String json(String input) {
        return input.replaceAll("'", "\"");
    }

}
