package org.lilycms.rest;

import org.lilycms.repository.api.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/schema/recordTypeById")
public class RecordTypeByIdCollectionResource extends RepositoryEnabled {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<RecordType> postAction) {

        if (!postAction.getAction().equals("create")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        TypeManager typeManager = repository.getTypeManager();

        RecordType recordType = postAction.getEntity();
        try {
            recordType = typeManager.createRecordType(recordType);
        } catch (RecordTypeExistsException e) {
            throw new ResourceException(e, CONFLICT.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating record type.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        URI uri = UriBuilder.fromResource(RecordTypeByIdAndVersionResource.class).build(recordType.getId(), recordType.getVersion());
        return Response.created(uri).entity(recordType).build();
    }

}