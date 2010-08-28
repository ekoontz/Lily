package org.lilycms.rest;

import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RepositoryException;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/record")
public class RecordCollectionResource extends RepositoryEnabled {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<Record> postAction) {
        if (!postAction.getAction().equals("create")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        Record record = postAction.getEntity();

        try {
            // TODO record we respond with should be full record or be limited to user-specified field list
            record = repository.create(record);
            URI uri = UriBuilder.fromResource(RecordResource.class).build(record.getId());
            return Response.created(uri).entity(record).build();
        } catch (RepositoryException e) {
            throw new ResourceException(e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}

