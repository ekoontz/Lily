package org.lilycms.rest;

import org.lilycms.repository.api.QName;

import javax.ws.rs.core.MultivaluedMap;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

public class ResourceClassUtil {
    public static QName parseQName(String name, MultivaluedMap<String, String> queryParams) {
        int pos = name.indexOf('$');
        if (pos == -1) {
            throw new ResourceException("Invalid qualified name: " + name, BAD_REQUEST.getStatusCode());
        }

        String prefix = name.substring(0, pos);
        String localName = name.substring(pos + 1);

        String uri = queryParams.getFirst("ns." + prefix);
        if (uri == null) {
            throw new ResourceException("Undefined prefix in qualified name: " + name, BAD_REQUEST.getStatusCode());
        }

        return new QName(uri, localName);
    }
}
