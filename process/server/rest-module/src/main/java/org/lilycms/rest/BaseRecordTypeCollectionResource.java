package org.lilycms.rest;

import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.TypeManager;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public abstract class BaseRecordTypeCollectionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public EntityList<RecordType> get() {
        try {
            return new EntityList<RecordType>(repository.getTypeManager().getRecordTypes());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record type list.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    protected RecordType processPost(PostAction<RecordType> postAction) {
        if (!postAction.getAction().equals("create")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        TypeManager typeManager = repository.getTypeManager();

        RecordType recordType = postAction.getEntity();
        try {
            recordType = typeManager.createRecordType(recordType);
        } catch (RecordTypeExistsException e) {
            throw new ResourceException(e, CONFLICT.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating record type.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        return recordType;
    }
}
