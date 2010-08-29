package org.lilycms.rest;

import org.lilycms.repository.api.*;
import org.lilycms.rest.import_.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/schema/recordType/{name}")
public class RecordTypeResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public RecordType get(@PathParam("name") String name, @Context UriInfo uriInfo) {
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());
        try {
            return repository.getTypeManager().getRecordTypeByName(qname, null);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading record type with name " + qname, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("name") String name, RecordType recordType, @Context UriInfo uriInfo) {
        // Since the name can be updated, in this case we allow that the name in the submitted record type
        // is different from the name in the URI.
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());

        ImportResult<RecordType> result;
        try {
            result = RecordTypeImport.importRecordType(recordType, ImportMode.CREATE_OR_UPDATE, IdentificationMode.NAME,
                    qname, repository.getTypeManager());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating or updating record type named " + qname, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        recordType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = UriBuilder.fromResource(RecordTypeByIdAndVersionResource.class).build(recordType.getId(), recordType.getVersion());
                response = Response.created(uri).entity(recordType).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                response = Response.ok(recordType).build();
                break;
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }
}
