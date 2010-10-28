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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

public class NamespacesConverter {
    public static Namespaces fromContextJson(ObjectNode contextNode) throws JsonFormatException {
        Namespaces namespaces;
        JsonNode nsNode = contextNode.get("namespaces");
        if (nsNode == null) {
            namespaces = new Namespaces();
        } else if (!nsNode.isObject()) {
            throw new JsonFormatException("The value of the namespaces property should be an object.");
        } else {
            namespaces = NamespacesConverter.fromJson((ObjectNode)nsNode);
        }

        return namespaces;
    }

    public static Namespaces fromJson(ObjectNode nsNode) throws JsonFormatException {
        Namespaces namespaces = new Namespaces();

        Iterator<Map.Entry<String, JsonNode>> fieldsIt = nsNode.getFields();
        while (fieldsIt.hasNext()) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();

            String namespace = entry.getKey();
            String prefix;

            if (!entry.getValue().isTextual()) {
                throw new JsonFormatException("Namespace property should map to a string prefix. Namespace: " +
                        namespace);
            } else {
                prefix = entry.getValue().getTextValue();
            }

            // addMapping will validate that the same prefix is not already bound to another namespace.
            namespaces.addMapping(prefix, namespace);
        }

        return namespaces;
    }

    public static JsonNode toJson(Namespaces namespaces) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode jsonNamespaces = factory.objectNode();

        for (Map.Entry<String, String> entry : namespaces.getNsToPrefixMapping().entrySet()) {
            String namespace = entry.getKey();
            String prefix = entry.getValue();

            jsonNamespaces.put(namespace, prefix);
        }

        return jsonNamespaces;
    }
}
