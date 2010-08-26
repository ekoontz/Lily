package org.lilycms.rest.test;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.lilycms.testfw.HBaseProxy;
import org.lilycms.util.io.Closer;
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
    public void test() throws Exception {
        // Create field type
        String body = json("{action: 'create', fieldType: {name: 'b$title', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);

        assertEquals(Status.SUCCESS_CREATED, response.getStatus());

        JsonNode jsonNode = readJson(response.getEntity());
        String prefix = jsonNode.get("namespaces").get("org.lilycms.resttest").getTextValue();
        assertEquals(prefix + "$title", jsonNode.get("name").getTextValue());
        String titleFieldId = jsonNode.get("id").getTextValue();
        assertEquals(BASE_URI + "/schema/fieldTypeById/" + titleFieldId, response.getLocationRef().toString());

        // Read the field type
        response = get(response.getLocationRef().toString());
        assertEquals(Status.SUCCESS_OK, response.getStatus());

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'b$book', fields: [ {name: 'b$title'} ]," +
                "namespaces: { 'org.lilycms.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);

        assertEquals(Status.SUCCESS_CREATED, response.getStatus());

        jsonNode = readJson(response.getEntity());
        prefix = jsonNode.get("namespaces").get("org.lilycms.resttest").getTextValue();
        assertEquals(prefix + "$book", jsonNode.get("name").getTextValue());

        // Read the record type
        response = get(response.getLocationRef().toString());
        assertEquals(Status.SUCCESS_OK, response.getStatus());

        // Create a record
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.faster_fishing", body);

        assertEquals(Status.SUCCESS_CREATED, response.getStatus());

        // Read the record
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertEquals(Status.SUCCESS_OK, response.getStatus());

        // Update the field type name
        body = json("{name: 'b$title2', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'b' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + titleFieldId, body);

        assertEquals(Status.SUCCESS_OK, response.getStatus());

        response = get(BASE_URI + "/schema/fieldType/b$title2?ns.b=org.lilycms.resttest");
        assertEquals(Status.SUCCESS_OK, response.getStatus());
    }

    @Test
    public void testBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob1', valueType: { primitive: 'BLOB' }, " +
                "scope: 'versioned', namespaces: { 'org.lilycms.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertEquals(Status.SUCCESS_CREATED, response.getStatus());

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT', fields: [ {name: 'b$blob1'} ]," +
                "namespaces: { 'org.lilycms.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertEquals(Status.SUCCESS_CREATED, response.getStatus());

        // Upload a blob
        Request req = new Request(Method.POST, BASE_URI + "/blob");
        String data = "Hello, blob world!";
        Representation blobRepr = new StringRepresentation(data, MediaType.TEXT_PLAIN);
        req.setEntity(blobRepr);
        response = CLIENT.handle(req);

        assertEquals(Status.SUCCESS_CREATED, response.getStatus());
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
        assertEquals(Status.SUCCESS_CREATED, response.getStatus());

        // Read the record
        response = get(BASE_URI + "/record/USER.blob1");
        assertEquals(Status.SUCCESS_OK, response.getStatus());

        jsonNode = readJson(response.getEntity());
        // TODO verify again

        // Read the blob
        response = get(BASE_URI + "/record/USER.blob1/field/b$blob1/data?ns.b=org.lilycms.resttest");
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        // TODO verify content equals what was uploaded
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
