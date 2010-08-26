package org.lilycms.rest.providers.json;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonFormat {
    public static void serialize(JsonNode jsonNode, OutputStream outputStream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(outputStream, jsonNode);
    }

    public static JsonNode deserialize(InputStream inputStream) throws IOException {
        JsonFactory jsonFactory = new MappingJsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JsonParser jp = jsonFactory.createJsonParser(inputStream);

        return jp.readValueAsTree();
    }
}
