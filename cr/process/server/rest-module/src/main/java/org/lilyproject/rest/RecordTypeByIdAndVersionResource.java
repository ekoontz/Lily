package org.lilyproject.rest;

import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.TypeException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("schema/recordTypeById/{id}/version/{version:\\d+}")
public class RecordTypeByIdAndVersionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public RecordType get(@PathParam("id") String id, @PathParam("version") Long version) {
        try {
            return repository.getTypeManager().getRecordTypeById(id, version);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading record type with id " + id + ", version " + version, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
