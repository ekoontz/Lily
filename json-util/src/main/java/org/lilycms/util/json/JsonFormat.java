package org.lilycms.util.json;

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
    public static final ObjectMapper OBJECT_MAPPER_NON_STD = new ObjectMapper();
    static {
        // These non-standard features (could) make parsing slightly slower.
        OBJECT_MAPPER_NON_STD.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        OBJECT_MAPPER_NON_STD.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER_NON_STD.getDeserializationConfig().enable(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS);
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

    public static JsonNode deserializeNonStd(InputStream inputStream) throws IOException {
        return OBJECT_MAPPER_NON_STD.readTree(inputStream);
    }

    public static JsonNode deserialize(byte[] data) throws IOException {
        return OBJECT_MAPPER.readTree(new ByteArrayInputStream(data));
    }

    public static JsonNode deserializeNonStd(byte[] data) throws IOException {
        return OBJECT_MAPPER_NON_STD.readTree(new ByteArrayInputStream(data));
    }

    public static void setNonStdFeatures(MappingJsonFactory jsonFactory) {
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonFactory.getCodec().getDeserializationConfig().enable(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS);
    }
}
