package org.lilycms.rest;

import org.lilycms.repository.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.*;

@Path("/schema/fieldTypeById")
public class FieldTypeByIdCollectionResource extends RepositoryEnabled {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<FieldType> postAction) {

        if (!postAction.getAction().equals("create")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        TypeManager typeManager = repository.getTypeManager();

        FieldType fieldType = postAction.getEntity();
        try {
            fieldType = typeManager.createFieldType(fieldType);
        } catch (FieldTypeExistsException e) {
            throw new ResourceException(e, CONFLICT.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error creating field type.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        URI uri = UriBuilder.fromResource(FieldTypeByIdResource.class).build(fieldType.getId());
        return Response.created(uri).entity(fieldType).build();
    }

}
