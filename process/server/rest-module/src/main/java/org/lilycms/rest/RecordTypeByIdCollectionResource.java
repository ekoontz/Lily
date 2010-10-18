package org.lilycms.rest;

import org.lilycms.repository.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("schema/recordTypeById")
public class RecordTypeByIdCollectionResource extends BaseRecordTypeCollectionResource {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<RecordType> postAction) {
        RecordType recordType = processPost(postAction);
        URI uri = UriBuilder.fromResource(RecordTypeByIdResource.class).build(recordType.getId());
        return Response.created(uri).entity(recordType).build();
    }

}
