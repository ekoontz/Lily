package org.lilycms.rest;

import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.core.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.*;

@Path("/schema/fieldTypeById/{id}")
public class FieldTypeByIdResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public FieldType get(@PathParam("id") String id) {
        try {
            return repository.getTypeManager().getFieldTypeById(id);
        } catch (FieldTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading field type with id " + id, e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("id") String id, FieldType fieldType) {
        if (fieldType.getId() != null && !fieldType.getId().equals(id)) {
            throw new ResourceException("Field type id in submitted field type does not match the id in URI.",
                    BAD_REQUEST.getStatusCode());
        }
        fieldType.setId(id);

        ImportResult<FieldType> result;
        try {
            result = FieldTypeImport.importFieldType(fieldType, ImportMode.UPDATE, IdentificationMode.ID,
                    null, repository.getTypeManager());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating or updating field type with id " + id, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        fieldType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case UPDATED:
            case UP_TO_DATE:
                response = Response.ok(fieldType).build();
                break;
            case CANNOT_UPDATE_DOES_NOT_EXIST:
                throw new ResourceException("Field type not found: " + id, NOT_FOUND.getStatusCode());
            case CONFLICT:
                throw new ResourceException(String.format("Field type %1$s exists but with %2$s %3$s instead of %4$s",
                        id, result.getConflictingProperty(), result.getConflictingOldValue(),
                        result.getConflictingNewValue()), CONFLICT.getStatusCode());
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }

}
