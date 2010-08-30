package org.lilycms.rest;

import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.core.ImportMode;
import org.lilycms.tools.import_.core.ImportResult;
import org.lilycms.tools.import_.core.ImportResultType;
import org.lilycms.tools.import_.core.RecordImport;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.*;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;

import static javax.ws.rs.core.Response.Status.*;

@Path("/record/{id}")
public class RecordResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public Record get(@PathParam("id") String id, @Context UriInfo uriInfo) {
        RecordId recordId = repository.getIdGenerator().fromString(id);
        List<QName> fieldQNames = ResourceClassUtil.parseFieldList(uriInfo);
        try {
            return repository.read(recordId, fieldQNames);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("id") String id, Record record) {
        RecordId recordId = repository.getIdGenerator().fromString(id);

        if (record.getId() != null && !record.getId().equals(recordId)) {
            throw new ResourceException("Record id in submitted record does not match record id in URI.",
                    BAD_REQUEST.getStatusCode());
        }

        record.setId(recordId);

        ImportResult<Record> result;
        try {
            result = RecordImport.importRecord(record, ImportMode.CREATE_OR_UPDATE, repository);
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating or updating record type with id " + id, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        // TODO record we respond with should be full record or be limited to user-specified field list
        record = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = UriBuilder.fromResource(RecordResource.class).build(record.getId());
                response = Response.created(uri).entity(record).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                response = Response.ok(record).build();
                break;
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }

    @POST
    @Produces("application/json")
    @Consumes("application/json")
    public Record post(@PathParam("id") String id, PostAction<Record> postAction) {
        if (!postAction.getAction().equals("update")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        RecordId recordId = repository.getIdGenerator().fromString(id);
        Record record = postAction.getEntity();

        if (record.getId() != null && !record.getId().equals(recordId)) {
            throw new ResourceException("Record id in submitted record does not match record id in URI.",
                    BAD_REQUEST.getStatusCode());
        }

        record.setId(recordId);

        try {
            // TODO record we respond with should be full record or be limited to user-specified field list
            return repository.update(record);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException(e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @DELETE
    public Response delete(@PathParam("id") String id) {
        RecordId recordId = repository.getIdGenerator().fromString(id);
        try {
            repository.delete(recordId);
        } catch (RecordNotFoundException e) {
            // ignore: delete is idempotent!
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return Response.status(OK.getStatusCode()).build();
    }
}
