package org.lilycms.rest;

import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RepositoryException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/record/{id}/variant")
public class RecordVariantCollectionResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public RecordList get(@PathParam("id") String id) {
        RecordId recordId = repository.getIdGenerator().fromString(id);
        try {
            Set<RecordId> recordIds = repository.getVariants(recordId);

            List<Record> records = new ArrayList<Record>();
            for (RecordId variant : recordIds) {
                records.add(repository.newRecord(variant));
            }
            return new RecordList(records);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record variants.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

    }
}
