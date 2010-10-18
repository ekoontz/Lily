package org.lilycms.rest;

import org.lilycms.repository.api.FieldType;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

@Path("schema/fieldType")
public class FieldTypeCollectionResource extends BaseFieldTypeCollectionResource {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<FieldType> postAction) {
        FieldType fieldType = processPost(postAction);
        URI uri = UriBuilder.fromResource(FieldTypeResource.class).
                queryParam("ns.n", fieldType.getName().getNamespace()).
                build("n$" + fieldType.getName().getName());
        return Response.created(uri).entity(fieldType).build();
    }

}

