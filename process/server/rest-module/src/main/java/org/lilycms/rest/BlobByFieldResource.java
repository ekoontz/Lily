package org.lilycms.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/record/{id}/field/{fieldName}/data")
public class BlobByFieldResource extends RepositoryEnabled {

    @GET
    @Produces("*/*")
    public Response get(@PathParam("id") String id, @PathParam("fieldName") String fieldName, @Context UriInfo uriInfo) {
        return BlobByVersionAndFieldResource.getBlob(id, null, fieldName, uriInfo, repository);
    }

}
