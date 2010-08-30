package org.lilycms.rest;

import org.lilycms.repository.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

@Path("/schema/fieldTypeById")
public class FieldTypeByIdCollectionResource extends BaseFieldTypeCollectionResource {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<FieldType> postAction) {
        FieldType fieldType = processPost(postAction);
        URI uri = UriBuilder.fromResource(FieldTypeByIdResource.class).build(fieldType.getId());
        return Response.created(uri).entity(fieldType).build();
    }

}
