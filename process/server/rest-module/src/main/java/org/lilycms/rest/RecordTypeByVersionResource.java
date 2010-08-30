package org.lilycms.rest;

import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.TypeException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/schema/recordType/{name}/version/{version:\\d+}")
public class RecordTypeByVersionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public RecordType get(@PathParam("name") String name, @PathParam("version") long version, @Context UriInfo uriInfo) {
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());
        try {
            return repository.getTypeManager().getRecordTypeByName(qname, version);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading record type with name " + qname + ", version " + version, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
