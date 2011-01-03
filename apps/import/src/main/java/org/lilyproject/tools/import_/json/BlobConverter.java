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
package org.lilyproject.tools.import_.json;

import net.iharder.Base64;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.util.json.JsonUtil;

import java.io.IOException;

public class BlobConverter {
    public static ObjectNode toJson(Blob blob) {
        ObjectNode jsonBlob = JsonNodeFactory.instance.objectNode();
        jsonBlob.put("value", valueToString(blob.getValue()));
        jsonBlob.put("mediaType", blob.getMediaType());
        if (blob.getName() != null)
            jsonBlob.put("name", blob.getName());
        jsonBlob.put("size", blob.getSize());
        return jsonBlob;
    }

    public static Blob fromJson(ObjectNode node) {
        String mediaType = JsonUtil.getString(node, "mediaType", null);
        long size = JsonUtil.getLong(node, "size");
        String name = JsonUtil.getString(node, "name", null);
        byte[] value = valueFromString(JsonUtil.getString(node, "value"));

        Blob blob = new Blob(mediaType, size, name);
        blob.setValue(value);

        return blob;
    }

    public static String valueToString(byte[] value) {
        // URL safe encoding because the value (= the blob access key) might be embedded in URIs
        try {
            return Base64.encodeBytes(value, Base64.URL_SAFE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] valueFromString(String value) {
        try {
            return Base64.decode(value, Base64.URL_SAFE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
