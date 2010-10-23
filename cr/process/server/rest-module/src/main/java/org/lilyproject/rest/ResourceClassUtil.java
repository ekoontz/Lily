package org.lilyproject.rest;

import org.lilyproject.repository.api.QName;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.List;

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

    public static List<QName> parseFieldList(UriInfo uriInfo) {
        String fields = uriInfo.getQueryParameters().getFirst("fields");
        List<QName> fieldQNames = null;
        if (fields != null) {
            fieldQNames = new ArrayList<QName>();
            String[] fieldParts = fields.split(",");
            for (String field : fieldParts) {
                field = field.trim();
                if (field.length() == 0)
                    continue;

                fieldQNames.add(ResourceClassUtil.parseQName(field, uriInfo.getQueryParameters()));
            }
        }
        return fieldQNames;
    }
}
