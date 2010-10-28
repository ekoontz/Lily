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

import org.lilyproject.repository.api.QName;

public class QNameConverter {
    public static QName fromJson(String name, Namespaces namespaces) throws JsonFormatException {
        int pos = name.indexOf('$');
        if (pos == -1) {
            throw new JsonFormatException("Invalid qualified name, does not contain a $: " + name);
        }

        String prefix = name.substring(0, pos);
        String localName = name.substring(pos + 1);
        String uri = namespaces.getNamespace(prefix);
        if (uri == null) {
            throw new JsonFormatException("Undefined prefix in qualified name: " + name);
        }

        return new QName(uri, localName);
    }

    public static String toJson(QName qname, Namespaces namespaces) {
        return namespaces.getOrMakePrefix(qname.getNamespace()) + "$" + qname.getName();
    }
}
