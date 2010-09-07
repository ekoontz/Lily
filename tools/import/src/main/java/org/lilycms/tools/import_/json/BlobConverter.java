package org.lilycms.tools.import_.json;

import net.iharder.Base64;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.Blob;
import org.lilycms.util.json.JsonUtil;

import java.io.IOException;

public class BlobConverter {
    public static ObjectNode toJson(Blob blob) {
        ObjectNode jsonBlob = JsonNodeFactory.instance.objectNode();
        jsonBlob.put("value", valueToString(blob.getValue()));
        jsonBlob.put("mimeType", blob.getMimetype());
        if (blob.getName() != null)
            jsonBlob.put("name", blob.getName());
        jsonBlob.put("size", blob.getSize());
        return jsonBlob;
    }

    public static Blob fromJson(ObjectNode node) {
        String mimeType = JsonUtil.getString(node, "mimeType", null);
        long size = JsonUtil.getLong(node, "size");
        String name = JsonUtil.getString(node, "name", null);
        byte[] value = valueFromString(JsonUtil.getString(node, "value"));

        Blob blob = new Blob(mimeType, size, name);
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
            return Base64.decode(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
