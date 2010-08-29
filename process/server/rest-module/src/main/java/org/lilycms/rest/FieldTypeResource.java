package org.lilycms.rest;

import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.core.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.*;

@Path("/schema/fieldType/{name}")
public class FieldTypeResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public FieldType get(@PathParam("name") String name, @Context UriInfo uriInfo) {
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());
        try {
            return repository.getTypeManager().getFieldTypeByName(qname);
        } catch (FieldTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading field type with name " + qname, e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("name") String name, FieldType fieldType, @Context UriInfo uriInfo) {
        // Since the name can be updated, in this case we allow that the name in the submitted field type
        // is different from the name in the URI.
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());

        ImportResult<FieldType> result;
        try {
            result = FieldTypeImport.importFieldType(fieldType, ImportMode.CREATE_OR_UPDATE, IdentificationMode.NAME,
                    qname, repository.getTypeManager());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating or updating field type named " + qname, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        fieldType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = UriBuilder.fromResource(FieldTypeByIdResource.class).build(fieldType.getId());
                response = Response.created(uri).entity(fieldType).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                // TODO if the name was updated, maybe we should set a Location header as well?
                response = Response.ok(fieldType).build();
                break;
            case CONFLICT:
                throw new ResourceException(String.format("Field type %1$s exists but with %2$s %3$s instead of %4$s",
                        qname, result.getConflictingProperty(), result.getConflictingOldValue(),
                        result.getConflictingNewValue()), CONFLICT.getStatusCode());
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }

}
