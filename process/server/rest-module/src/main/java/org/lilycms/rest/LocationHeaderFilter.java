package org.lilycms.rest;

import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Reference;
import org.restlet.routing.Filter;

/**
 * This filter is a fix to work around incorrect location header construction logic
 * in Restlet and/or its JAX-RS implementation.
 */
public class LocationHeaderFilter extends Filter {
    @Override
    protected int doHandle(Request request, Response response) {
        int result = super.doHandle(request, response);

        if (response.getLocationRef() != null) {
            Reference locationRef = response.getLocationRef();
            String path = locationRef.getPath();
            String query = locationRef.getQuery();
            if (query != null)
                path = path + "?" + query;

            response.setLocationRef("service:/main" + path);
        }

        return result;
    }
}
