/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
