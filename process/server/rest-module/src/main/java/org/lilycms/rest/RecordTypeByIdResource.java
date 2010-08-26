package org.lilycms.rest;

import org.lilycms.repository.api.*;
import org.lilycms.rest.import_.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.*;

@Path("/schema/recordTypeById/{id}")
public class RecordTypeByIdResource {
    private Repository repository;

    @GET
    @Produces("application/json")
    public RecordType get(@PathParam("id") String id) {
        try {
            return repository.getTypeManager().getRecordTypeById(id, null);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading record type with id " + id, e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("id") String id, RecordType recordType) {
        if (recordType.getId() != null && !recordType.getId().equals(id)) {
            throw new ResourceException("Record type id in submitted field type does not match the id in URI.",
                    BAD_REQUEST.getStatusCode());
        }
        recordType.setId(id);

        ImportResult<RecordType> result;
        try {
            result = RecordTypeImport.importRecordType(recordType, ImportMode.UPDATE, IdentificationMode.ID,
                    repository.getTypeManager());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating or updating record type with id " + id, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        recordType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = UriBuilder.fromResource(RecordTypeByIdResource.class).build(recordType.getId());
                response = Response.created(uri).entity(recordType).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                response = Response.ok(recordType).build();
                break;
            case CANNOT_UPDATE_DOES_NOT_EXIST:
                throw new ResourceException("Record type not found: " + id, NOT_FOUND.getStatusCode());
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
