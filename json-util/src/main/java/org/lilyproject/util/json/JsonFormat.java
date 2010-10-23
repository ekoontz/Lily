package org.lilyproject.util.json;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;

/**
 * Json serialization & deserialization to/from Jackson's generic tree model.
 */
public class JsonFormat {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();

    public static final MappingJsonFactory JSON_FACTORY_NON_STD;
    static {
        JSON_FACTORY_NON_STD = new MappingJsonFactory();
        JSON_FACTORY_NON_STD.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_FACTORY_NON_STD.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JSON_FACTORY_NON_STD.getCodec().getDeserializationConfig().enable(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    public static void serialize(JsonNode jsonNode, OutputStream outputStream) throws IOException {
        OBJECT_MAPPER.writeValue(outputStream, jsonNode);
    }

    public static byte[] serializeAsBytes(JsonNode jsonNode) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(jsonNode);
    }

    public static JsonNode deserialize(InputStream inputStream) throws IOException {
        JsonParser jp = JSON_FACTORY.createJsonParser(inputStream);
        return jp.readValueAsTree();
    }

    public static JsonNode deserializeNonStd(InputStream inputStream) throws IOException {
        JsonParser jp = JSON_FACTORY_NON_STD.createJsonParser(inputStream);
        return jp.readValueAsTree();
    }

    public static JsonNode deserialize(byte[] data) throws IOException {
        JsonParser jp = JSON_FACTORY.createJsonParser(data);
        return jp.readValueAsTree();
    }

    public static JsonNode deserializeNonStd(byte[] data) throws IOException {
        JsonParser jp = JSON_FACTORY_NON_STD.createJsonParser(data);
        return jp.readValueAsTree();
    }
}
