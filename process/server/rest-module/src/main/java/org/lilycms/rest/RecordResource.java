package org.lilycms.rest;

import org.lilycms.repository.api.*;
import org.lilycms.rest.import_.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import javax.ws.rs.*;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.*;

@Path("/record/{id}")
public class RecordResource {
    private Repository repository;

    @GET
    @Produces("application/json")
    public Record get(@PathParam("id") String id) {
        RecordId recordId = repository.getIdGenerator().fromString(id);
        Record record;
        try {
            record = repository.read(recordId);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return record;
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

    @Autowired
    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
