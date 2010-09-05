package org.lilycms.tools.import_.json;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.Blob;
import org.lilycms.util.json.JsonUtil;

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
        return Base64.encodeBase64URLSafeString(value);
    }

    public static byte[] valueFromString(String value) {
        return Base64.decodeBase64(value);
    }


}
