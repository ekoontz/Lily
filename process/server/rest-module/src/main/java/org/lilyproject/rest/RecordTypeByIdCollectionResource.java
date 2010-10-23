package org.lilyproject.rest;

import org.lilyproject.repository.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

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
