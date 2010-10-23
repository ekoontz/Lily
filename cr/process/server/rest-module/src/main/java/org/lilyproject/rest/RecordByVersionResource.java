package org.lilyproject.rest;

import org.lilyproject.repository.api.*;

import javax.ws.rs.*;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/version/{version:\\d+}")
public class RecordByVersionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public Record get(@PathParam("id") String id, @PathParam("version") Long version) {
        RecordId recordId = repository.getIdGenerator().fromString(id);
        try {
            return repository.read(recordId, version);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (VersionNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    // TODO implement updating of versioned-mutable data (PUT or POST?)

}
