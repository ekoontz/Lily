package org.lilycms.util.json;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonFormat {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final MappingJsonFactory JSON_FACTORY;
    static {
        JSON_FACTORY = new MappingJsonFactory();
        JSON_FACTORY.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_FACTORY.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JSON_FACTORY.getCodec().getDeserializationConfig().enable(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    public static void serialize(JsonNode jsonNode, OutputStream outputStream) throws IOException {
        OBJECT_MAPPER.writeValue(outputStream, jsonNode);
    }

    public static byte[] serializeAsBytes(JsonNode jsonNode) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(jsonNode);
    }

    public static JsonNode deserialize(InputStream inputStream) throws IOException {
        return OBJECT_MAPPER.readTree(inputStream);
    }

    public static JsonNode deserialize(byte[] data) throws IOException {
        return OBJECT_MAPPER.readTree(new ByteArrayInputStream(data));
    }
}
