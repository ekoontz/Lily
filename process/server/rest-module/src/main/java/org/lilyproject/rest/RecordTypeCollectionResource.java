package org.lilyproject.rest;

import org.lilyproject.repository.api.RecordType;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

@Path("schema/recordType")
public class RecordTypeCollectionResource extends BaseRecordTypeCollectionResource {

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public Response post(PostAction<RecordType> postAction) {
        RecordType recordType = processPost(postAction);
        URI uri = UriBuilder.fromResource(RecordTypeResource.class).
                queryParam("ns.n", recordType.getName().getNamespace()).
                build("n$" + recordType.getName().getName());
        return Response.created(uri).entity(recordType).build();
    }

}
